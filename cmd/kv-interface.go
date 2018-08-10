package cmd

type KVAPI interface {
	Put(container string, key string, value []byte) error
	Get(container string, key string, value []byte) error
	Delete(container string, key string) error
	List() ([]string, error)
}
