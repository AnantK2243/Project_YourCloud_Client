# Storage Node Daemon

## Overview

The Storage Node Daemon is a headless service designed to operate as a reliable storage unit in a distributed, self-hosted cloud storage system. It runs on Linux and manages encrypted data chunks, ensuring secure communication with a central backend server.

## Features

-   **Headless Operation**: Runs without direct user interaction.
-   **Encrypted Data Handling**: Only stores and retrieves pre-encrypted data.
-   **Proactive Monitoring**: Monitors storage usage and physical disk space.
-   **Secure Communication**: Uses WebSocket for secure communication with the backend.

## Project Structure

```
storage_node_daemon
├── src
│   ├── main.rs          # Entry point of the application
│   ├── config.rs        # Configuration handling
│   ├── storage.rs       # Storage management functions
│   ├── network.rs       # WebSocket connection management
│   ├── commands.rs      # Command handling logic
│   └── lib.rs           # Library module for shared types/functions
├── config
│   └── config.toml      # Configuration settings
├── systemd
│   └── storage_node_daemon.service # Systemd service configuration
├── scripts
│   └── install.sh       # Installation script
├── Cargo.toml           # Cargo configuration file
├── Cargo.lock           # Dependency lock file
├── README.md            # Project documentation
└── .gitignore           # Git ignore file
```

## Setup Instructions

1. **Clone the Repository**:

    ```
    git clone https://github.com/your-repo/storage_node_daemon.git
    cd storage_node_daemon
    ```

2. **Build the Project**:

    ```
    cargo build --release
    ```

3. **Configure the Daemon**:
   Edit the `config/config.toml` file to set your node ID, authentication token, backend URLs, storage path, and limits.

4. **Install the Daemon**:
   Run the installation script to set up the service:

    ```
    ./scripts/install.sh
    ```

5. **Start the Service**:
   Enable and start the service using systemd:
    ```
    sudo systemctl enable storage_node_daemon
    sudo systemctl start storage_node_daemon
    ```

## Usage

The Storage Node Daemon will automatically register with the backend and start listening for commands. It will handle storage operations such as storing, retrieving, and deleting encrypted data chunks as instructed by the backend.

## Logging

Logs are generated based on the configured log level in `config/config.toml`. Ensure to check the logs for any issues or operational messages.

## Security Considerations

-   Ensure that the configuration file has restrictive permissions.
-   Use secure URLs for backend communication.
-   Regularly monitor disk usage and health status.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
