package gmq

type HandleFunc func(msg *Msg) error
type Middleware func(handleFunc HandleFunc) HandleFunc
