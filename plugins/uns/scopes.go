package uns

// Integration scopes a personal access token can carry. A door names the one
// scope a token needs there; JWTs are not scope-checked, their grants decide.
// The local door accepts tokens scoped to the api, which forwards the token so
// the node authorizes the person.
const (
	ScopeAPI        = "api"
	ScopeI3X        = "i3x"
	ScopeMCP        = "mcp"
	ScopeBrokerHTTP = "broker-http"
	ScopeBrokerMQTT = "broker-mqtt"
)
