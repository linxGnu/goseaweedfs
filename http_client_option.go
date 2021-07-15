package goseaweedfs

type HttpClientOption func(client *httpClient)

func WithHttpClientAuthKey(authKey string) HttpClientOption {
	return func(client *httpClient) {
		client.authKey = authKey
	}
}
