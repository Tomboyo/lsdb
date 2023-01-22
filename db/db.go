package db

import "errors"

func Add(key, value string) error {
	return errors.New("not yet implemented")
}

func Get(key string) (*string, error) {
	return nil, errors.New("value not found")
}
