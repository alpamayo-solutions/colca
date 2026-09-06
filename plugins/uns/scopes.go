package uns

// Integration scopes a personal access token carries (the api's ApiKey
// scopes vocabulary). A door names the ONE scope a token must hold to be
// accepted there; a JWT is never scope-checked, its grants say what it may
// do. The local door accepts a forwarded token scoped to the api: the person
// authenticated at the api with it, and the api hands it through so the
// node authorizes the person (node-side command authorization design §3B).
// Requiring the human doors' scope there instead refused every token the api
// ever issues -- none carries broker-http by default -- and with it every
// Edit command made under a personal access token.
const (
	ScopeAPI        = "api"
	ScopeI3X        = "i3x"
	ScopeMCP        = "mcp"
	ScopeBrokerHTTP = "broker-http"
	ScopeBrokerMQTT = "broker-mqtt"
)
