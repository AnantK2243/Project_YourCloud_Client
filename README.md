# Project YourCloud Storage Client

A lightweight, command-line storage node client for the Project YourCloud distributed storage system. Allowing users to manage their own cloud on old or unused devices.

## Quick Start

### 1. Install

```bash
# Run the automated installer
./scripts/install.sh
```

The installer will:

-   Build the binary from source
-   Install it to `/usr/local/bin/yourcloud_client`
-   Run interactive setup to configure your node
-   Optionally create a systemd service

### 2. Get Credentials

1. Visit your Project YourCloud dashboard
2. Register a new storage node
3. Copy the Node ID and Auth Token provided

### 3. Configure

```bash
# Interactive setup (if not done during install)
yourcloud_client setup
```

### 4. Start

```bash
# Start as daemon
yourcloud_client start

# Or start as systemd service (if installed)
sudo systemctl start yourcloud-storage
```

## Commands

### Basic Commands

```bash
yourcloud_client start     # Start the storage node daemon
yourcloud_client status    # Display current status and configuration
yourcloud_client setup     # Interactive configuration setup
yourcloud_client validate  # Validate current configuration
yourcloud_client config    # Show configuration file and contents
yourcloud_client --help    # Display help information
```

### System Service Commands (if systemd service is installed)

```bash
sudo systemctl start yourcloud-storage      # Start service
sudo systemctl stop yourcloud-storage       # Stop service
sudo systemctl restart yourcloud-storage    # Restart service
sudo systemctl status yourcloud-storage     # Check service status
sudo journalctl -u yourcloud-storage -f     # View live logs
```

## Configuration

Configuration is stored in `~/.config/Project_YourCloud/config.toml`:

```toml
node_id = "your-node-id-from-dashboard"
auth_token = "your-auth-token-from-dashboard"
storage_path = "/path/to/storage/directory"
max_storage_gib = 40.0
ws_url = ""  # Leave empty for default backend
```

### Configuration Options

-   **node_id**: Unique identifier from your dashboard
-   **auth_token**: Authentication token from your dashboard
-   **storage_path**: Directory where chunks will be stored
-   **max_storage_gib**: Maximum storage to allocate (in GiB)
-   **ws_url**: Backend WebSocket URL (leave empty for default)

## Manual Installation

If you prefer to install manually:

```bash
# 1. Build the project
cargo build --release

# 2. Copy binary (optional)
sudo cp target/release/Project_YourCloud_Client /usr/local/bin/yourcloud_client

# 3. Configure
yourcloud_client setup

# 4. Start
yourcloud_client start
```

## Troubleshooting

### Common Issues

**Connection Problems**

```bash
# Check status and configuration
yourcloud_client status

# Validate configuration
yourcloud_client validate

# View logs (if using systemd)
sudo journalctl -u yourcloud-storage -f
```

**Build Issues**

-   Ensure Rust toolchain is installed: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
-   Update Rust: `rustup update`
-   Install build dependencies: `sudo apt install build-essential pkg-config libssl-dev`

**Storage Issues**

-   Verify storage directory exists and is writable
-   Check available disk space
-   Ensure storage path in config is absolute

### Getting Help

-   Check your node status: `yourcloud_client status`
-   Validate configuration: `yourcloud_client validate`
-   View help: `yourcloud_client --help`
-   Check service logs: `sudo journalctl -u yourcloud-storage -f`

## Uninstallation

To completely remove the storage client:

```bash
# Run the uninstall script
./scripts/uninstall.sh
```

The uninstall script will:

-   Stop and remove the systemd service
-   Remove the binary and configuration files
-   Optionally remove storage data (with confirmation)
-   Clean up logs and processes

## Security Notes

-   Configuration files contain sensitive authentication tokens
-   Storage directory should only be accessible by the service user
-   Regularly monitor logs for suspicious activity
-   Keep the client updated to the latest version

## Project Structure

```
storage_client/
├── src/
│   ├── main.rs          # Entry point and CLI handling
│   ├── cli.rs           # Command-line interface
│   ├── config.rs        # Configuration management
│   ├── storage.rs       # Storage operations
│   ├── network.rs       # WebSocket communication
│   └── commands.rs      # Backend command handling
├── scripts/
│   ├── install.sh       # Automated installer
│   ├── uninstall.sh     # Uninstaller script
│   └── README.md        # Scripts documentation
├── Cargo.toml           # Rust dependencies
└── README.md            # This file
```

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for enhancements or bug fixes.
