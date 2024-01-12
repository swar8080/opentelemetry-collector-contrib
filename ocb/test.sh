ocb --config builder-config.yaml
dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient --log exec ./otelcol-dev/otelcol-dev -- --config config.yaml;