package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"hash"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketName = []byte("hashes")
	hasherPool = sync.Pool{New: func() any { return sha256.New() }}
)

type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

func (g *flightGroup) Do(key string, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	c := &flightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}

type Cache struct {
	mu  sync.RWMutex
	mem map[string]string
	db  *bolt.DB
}

func NewCache(dbPath string, noSync bool) (*Cache, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{NoSync: noSync})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}

	c := &Cache{
		mem: make(map[string]string),
		db:  db,
	}

	_ = db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, v []byte) error {
			c.mem[string(k)] = string(v)
			return nil
		})
	})

	return c, nil
}

func (c *Cache) Close() error { return c.db.Close() }
func (c *Cache) Get(assetID string) (string, bool) {
	c.mu.RLock()
	h, ok := c.mem[assetID]
	c.mu.RUnlock()
	return h, ok
}

func (c *Cache) Set(assetID, hash string) {
	c.mu.Lock()
	c.mem[assetID] = hash
	c.mu.Unlock()

	_ = c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(assetID), []byte(hash))
	})
}

type AssetDeliveryResponse struct {
	Locations []struct {
		Location string `json:"location"`
	} `json:"locations"`
}

type RHIService struct {
	cache  *Cache
	client *http.Client
	flight flightGroup
}

func NewRHIService(cache *Cache, timeout time.Duration, maxIdle int) *RHIService {
	dialer := new(net.Dialer)
	dialer.Timeout = 5 * time.Second
	dialer.KeepAlive = 30 * time.Second

	return &RHIService{
		cache: cache,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        maxIdle,
				MaxIdleConnsPerHost: maxIdle,
				IdleConnTimeout:     90 * time.Second,
				DialContext:         dialer.DialContext,
				ForceAttemptHTTP2:   true,
			},
		},
	}
}

func (s *RHIService) GetHash(ctx context.Context, assetID string) (string, error) {
	if hash, ok := s.cache.Get(assetID); ok {
		return hash, nil
	}

	v, err := s.flight.Do(assetID, func() (any, error) {
		return s.fetchAndHash(ctx, assetID)
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (s *RHIService) fetchAndHash(ctx context.Context, assetID string) (string, error) {
	if hash, ok := s.cache.Get(assetID); ok {
		return hash, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://assetdelivery.roblox.com/v2/assetId/"+assetID, nil)
	if err != nil {
		return "", err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New("assetdelivery returned status " + resp.Status)
	}

	var data AssetDeliveryResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}
	if len(data.Locations) == 0 {
		return "", errors.New("No asset locations returned")
	}

	assetReq, err := http.NewRequestWithContext(ctx, http.MethodGet, data.Locations[0].Location, nil)
	if err != nil {
		return "", err
	}
	assetResp, err := s.client.Do(assetReq)
	if err != nil {
		return "", err
	}
	defer assetResp.Body.Close()

	if assetResp.StatusCode != http.StatusOK {
		return "", errors.New("asset download returned status " + assetResp.Status)
	}

	hasher := hasherPool.Get().(hash.Hash)
	defer func() {
		hasher.Reset()
		hasherPool.Put(hasher)
	}()

	if _, err := io.Copy(hasher, assetResp.Body); err != nil {
		return "", err
	}

	hashString := hex.EncodeToString(hasher.Sum(nil))
	s.cache.Set(assetID, hashString)

	return hashString, nil
}

func main() {
	var (
		port         = flag.String("port", "7771", "Server listen port")
		dbPath       = flag.String("db", "hashes.db", "BoltDB cache file path")
		timeout      = flag.Duration("timeout", 30*time.Second, "HTTP client timeout")
		readTimeout  = flag.Duration("read-timeout", 5*time.Second, "Server read timeout")
		writeTimeout = flag.Duration("write-timeout", 30*time.Second, "Server write timeout")
		idleTimeout  = flag.Duration("idle-timeout", 120*time.Second, "Server idle timeout")
		maxIdle      = flag.Int("max-idle", 77, "Max idle connections per host")
		noSync       = flag.Bool("nosync", false, "Skip fsync on BoltDB writes")
	)
	flag.Parse()

	cache, err := NewCache(*dbPath, *noSync)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	svc := NewRHIService(cache, *timeout, *maxIdle)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "You must provide an asset id in the url", http.StatusBadRequest)
			return
		}

		hash, err := svc.GetHash(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, hash)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "You must provide an asset id in the url", http.StatusBadRequest)
	})

	server := new(http.Server)
	server.Addr = ":" + *port
	server.Handler = mux
	server.ReadTimeout = *readTimeout
	server.WriteTimeout = *writeTimeout
	server.IdleTimeout = *idleTimeout

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
