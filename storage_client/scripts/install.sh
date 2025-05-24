#!/bin/bash

# Create a dedicated user and group for the storage node daemon
if ! id "storage_node_user" &> /dev/null; then
    sudo useradd -r -s /bin/false storage_node_user
fi

if ! getent group storage_node_group &>/dev/null; then
    sudo groupadd storage_node_group
fi

# Create the storage path directory
STORAGE_PATH="/var/lib/my_cloud_storage_node"
if [ ! -d "$STORAGE_PATH" ]; then
    sudo mkdir -p "$STORAGE_PATH"
    sudo chown storage_node_user:storage_node_group "$STORAGE_PATH"
    sudo chmod 750 "$STORAGE_PATH"
fi

# Copy the compiled binary to /usr/local/bin
BINARY_PATH="target/release/storage_node_daemon"
if [ -f "$BINARY_PATH" ]; then
    sudo cp "$BINARY_PATH" /usr/local/bin/
    sudo chown root:root /usr/local/bin/storage_node_daemon
    sudo chmod 755 /usr/local/bin/storage_node_daemon
else
    echo "Error: Binary not found at $BINARY_PATH. Please build the project first."
    exit 1
fi

# Copy the default configuration file
CONFIG_PATH="/etc/storage_node_daemon/config.toml"
if [ ! -f "$CONFIG_PATH" ]; then
    sudo cp config/config.toml "$CONFIG_PATH"
    sudo chown root:root "$CONFIG_PATH"
    sudo chmod 600 "$CONFIG_PATH"
fi

# Copy the systemd service file
SYSTEMD_SERVICE_PATH="/etc/systemd/system/storage_node_daemon.service"
if [ ! -f "$SYSTEMD_SERVICE_PATH" ]; then
    sudo cp systemd/storage_node_daemon.service "$SYSTEMD_SERVICE_PATH"
    sudo systemctl daemon-reload
    sudo systemctl enable storage_node_daemon
fi

echo "Installation completed. Please configure $CONFIG_PATH before starting the service."