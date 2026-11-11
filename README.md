# ForTunnels CLI

ForTunnels is an ngrok alternative that works in Russia: give localhost a public address for demos, webhook testing, and HTTP/HTTPS, TCP, or UDP services.

[Русская версия](README.ru.md) · [Website](https://fortunnels.ru/) · [Documentation](https://fortunnels.ru/docs)

## 1. Install

You don't need Go to use the CLI.

**macOS and Linux with Homebrew:**

```sh
brew install fortunnels/fortunnels/fortunnels
```

**Windows with Chocolatey** (run PowerShell as Administrator):

```powershell
choco install fortunnels
```

Without a package manager, [download a binary](https://fortunnels.ru/#downloads) for your OS and architecture. Extract it and put `fortunnels` (or `fortunnels.exe` on Windows) on your `PATH`. You can check the archive against [SHA256SUMS.txt](https://fortunnels.ru/landing-assets/downloads/SHA256SUMS.txt).

## 2. Open your first tunnel

Start your web app on port 8000, then run:

```sh
fortunnels 8000
```

Open the public HTTPS URL printed by the CLI. It forwards requests to `localhost:8000`. Keep the CLI running while you use the tunnel and press Ctrl+C to stop it. You can try an HTTP tunnel as a guest when guest access is enabled.

## 3. Register and connect your account

[Create a free account](https://fortunnels.ru/?register=1) to manage your tunnels from the dashboard. The Free plan includes CLI tokens.

1. Open [Dashboard → Settings](https://fortunnels.ru/dashboard/settings) and create a CLI token. The full token is shown once.
2. Copy the command from the dashboard or replace `YOUR_CLI_TOKEN` below with your token:

   ```sh
   fortunnels config add-authtoken YOUR_CLI_TOKEN
   ```

3. Run `fortunnels 8000` again. The CLI uses the saved token automatically, and the new tunnel appears in your dashboard.

Keep the token private. For automation, pass it through `FORTUNNELS_TOKEN` or `-token-file` instead of putting it in the command.

## CLI reference

| Task | Command |
| --- | --- |
| Share a local web app or API | `fortunnels 8000` |
| Receive webhooks in a local app | `fortunnels http 3000` |
| Forward to a local HTTPS service | `fortunnels https 8443` |
| Expose a local TCP service | `fortunnels tcp 22` |
| Expose a local UDP service | `fortunnels udp 9000` |
| Check the local token configuration | `fortunnels config check` |
| List all flags | `fortunnels --help` |

Sign in with a CLI token before opening TCP or UDP tunnels. Use the printed host and port to connect. Port shortcuts use `localhost` for HTTP, HTTPS, and TCP, and `127.0.0.1` for UDP. To select an exact address, use `-local host:port`, for example `fortunnels -local '[::1]:8000'`.

- `-watch` monitors tunnel status; `-watch-interval 5s` adjusts polling.
- `-token-file` and `-token-stdin` supply a token for one run. An explicit token takes precedence over the saved token.
- `-udp-listen` and `-udp-dst` together select reverse UDP mode. For transport options, see the [CLI documentation](https://fortunnels.ru/docs/cli).

If the public URL returns **502**, first check that `http://localhost:8000` works locally. If authentication fails, check the local config with `fortunnels config check`. That command doesn't contact the server; replace the token in Dashboard → Settings if it expired or was revoked.

## Build from source

From a clone of this repository, run the following with the Go toolchain specified in [go.mod](go.mod):

```sh
go build -o fortunnels ./cmd/client
```

On Windows, use `go build -o fortunnels.exe ./cmd/client`.

## License

The CLI is distributed under the [ForTunnels proprietary license](LICENSE).
