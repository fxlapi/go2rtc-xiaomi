package www

import "embed"

//go:embed *.html
//go:embed *.js
//go:embed schema.json
var Static embed.FS
