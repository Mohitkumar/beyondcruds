package middleware

import (
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

var _ MiddlewareFactory = (*AuthMiddlewareFactory)(nil)

type AuthMiddlewareFactory struct{}

func (m *AuthMiddlewareFactory) Name() string {
	return "auth"
}

func (m *AuthMiddlewareFactory) Create(cfg map[string]any) (Middleware, error) {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")
			if token == "" {
				http.Error(w, "missing Authorization token", 401)
				return
			}
			switch cfg["type"] {
			case "jwt":
				secret, ok := cfg["secret"]
				if !ok {
					http.Error(w, "missing secret", 401)
					return
				}
				valid, err := verifyToken(token, secret.(string))
				if err != nil || !valid {
					http.Error(w, "invalid token", 401)
					return
				}
			default:
				http.Error(w, "unsupported auth type", 401)
				return
			}

			next.ServeHTTP(w, r)
		})
	}, nil
}

func verifyToken(token string, key string) (bool, error) {
	parsedToken, err := jwt.Parse(token, func(t *jwt.Token) (any, error) {
		return []byte(key), nil
	})
	if err != nil {
		return false, err
	}

	return parsedToken.Valid, nil
}
