package cmd

type KVAPI interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	List() ([]string, error)
}
