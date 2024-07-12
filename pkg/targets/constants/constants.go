package constants

// Formats supported for generation
const (
	FormatInflux          = "influx"
	FormatTimescaleDB     = "timescaledb"
	FormatDatalayers      = "datalayers"
)

func SupportedFormats() []string {
	return []string{
		FormatInflux,
		FormatTimescaleDB,
		FormatDatalayers,
	}
}
