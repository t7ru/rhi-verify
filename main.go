package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("hashes")
var hasherPool = sync.Pool{New: func() any { return sha256.New() }}

type Cache struct {
	mu  sync.RWMutex
	mem map[string]string
	db  *bolt.DB
}

func NewCache(dbPath string) (*Cache, error) {
	db, err := bolt.Open(dbPath, 0600, new(bolt.Options))
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

	go func() {
		_ = c.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketName).Put([]byte(assetID), []byte(hash))
		})
	}()
}

type AssetDeliveryResponse struct {
	Locations []struct {
		Location string `json:"location"`
	} `json:"locations"`
}

type RHIService struct {
	cache  *Cache
	client *http.Client
}

func NewRHIService(cache *Cache) *RHIService {
	dialer := new(net.Dialer)
	dialer.Timeout = 5 * time.Second
	dialer.KeepAlive = 30 * time.Second

	return &RHIService{
		cache: cache,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        77,
				MaxIdleConnsPerHost: 77,
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
	cache, err := NewCache("hashes.db")
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	svc := NewRHIService(cache)
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
	server.Addr = ":7771"
	server.Handler = mux
	server.ReadTimeout = 5 * time.Second
	server.WriteTimeout = 30 * time.Second
	server.IdleTimeout = 120 * time.Second

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
