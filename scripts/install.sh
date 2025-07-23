#!/bin/bash

set -e  # Exit on any error

export PATH="$HOME/.cargo/bin:$PATH"

# Color codes for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Function to prompt for user input with default value
prompt_with_default() {
    local prompt="$1"
    local default="$2"
    local var_name="$3"
    
    read -p "$prompt [$default]: " input
    if [ -z "$input" ]; then
        eval "$var_name=\"$default\""
    else
        eval "$var_name=\"$input\""
    fi
}

# Function to validate required fields
validate_required() {
    local value="$1"
    local field_name="$2"
    
    if [ -z "$value" ]; then
        print_error "$field_name cannot be empty!"
        return 1
    fi
    return 0
}

print_info "Project YourCloud Storage Node Installer"
echo "========================================"

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    print_error "Cargo.toml not found. Please run this script from the Project_YourCloud_Client directory."
    exit 1
fi

# Step 1: Build the release binary
print_info "Building release binary..."
if ! cargo build --release; then
    print_error "Failed to build the project. Please check for compilation errors."
    exit 1
fi

BINARY_PATH="target/release/yourcloud_client"
if [ ! -f "$BINARY_PATH" ]; then
    print_error "Binary not found at $BINARY_PATH after build."
    exit 1
fi

print_success "Binary built successfully!"

# Step 2: Install binary
print_info "Installing binary to /usr/local/bin/yourcloud_client..."
sudo cp "$BINARY_PATH" /usr/local/bin/yourcloud_client
sudo chmod 755 /usr/local/bin/yourcloud_client
print_success "Binary installed successfully!"

# Step 3: Run setup

print_info "Running setup..."
yourcloud_client setup

# Step 4: Ask about systemd setup
echo ""
print_info "Service Setup"
echo "============="
read -p "Do you want to set up systemd service for automatic startup? (y/n) [y]: " SETUP_SYSTEMD
SETUP_SYSTEMD=${SETUP_SYSTEMD:-y}

if [[ "$SETUP_SYSTEMD" =~ ^[Yy]$ ]]; then
    # Create systemd service file
    SYSTEMD_SERVICE_PATH="/etc/systemd/system/yourcloud-storage.service"
    
    print_info "Creating systemd service..."
    sudo tee "$SYSTEMD_SERVICE_PATH" > /dev/null << EOF
[Unit]
Description=Project YourCloud Storage Node
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
Group=$USER
WorkingDirectory=$HOME
Environment=HOME=$HOME
ExecStart=/usr/local/bin/yourcloud_client start
ExecReload=/bin/kill -HUP \$MAINPID
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd and enable service
    sudo systemctl daemon-reload
    sudo systemctl enable yourcloud-storage.service
    
    print_success "Systemd service created and enabled!"
    
    # Ask if user wants to start the service now
    read -p "Do you want to start the service now? (y/n) [y]: " START_NOW
    START_NOW=${START_NOW:-y}
    
    if [[ "$START_NOW" =~ ^[Yy]$ ]]; then
        sudo systemctl start yourcloud-storage.service
        print_success "Service started!"
        print_info "You can check the service status with: sudo systemctl status yourcloud-storage"
        print_info "View logs with: sudo journalctl -u yourcloud-storage -f"
    else
        print_info "You can start the service later with: sudo systemctl start yourcloud-storage"
    fi
else
    print_info "Systemd service not configured. You can run the storage node manually."
fi

# Step 5: Final messages
print_success "Installation completed successfully!"
echo "===================================="
echo ""
print_info "Binary location: /usr/local/bin/yourcloud_client"
echo ""

print_info "Available Commands:"
echo "  yourcloud_client start     - Start the storage node daemon"
echo "  yourcloud_client status    - Display current storage node status and configuration"
echo "  yourcloud_client setup     - Interactive setup to configure the storage node"
echo "  yourcloud_client validate  - Validate the current configuration"
echo "  yourcloud_client config    - Show configuration file path and contents"
echo "  yourcloud_client --help    - Display help information"
echo ""

if [[ "$SETUP_SYSTEMD" =~ ^[Yy]$ ]]; then
    print_info "Systemd Commands:"
    echo "  Start service:   sudo systemctl start yourcloud-storage"
    echo "  Stop service:    sudo systemctl stop yourcloud-storage"
    echo "  Restart service: sudo systemctl restart yourcloud-storage"
    echo "  Check status:    sudo systemctl status yourcloud-storage"
    echo "  View logs:       sudo journalctl -u yourcloud-storage -f"
    echo ""
    print_info "Manual Commands:"
    echo "  Start manually:  yourcloud_client start"
    echo "  Check status:    yourcloud_client status"
    echo "  Reconfigure:     yourcloud_client setup"
fi

echo ""
print_info "Your storage node is ready! Check your dashboard to verify the connection."
echo ""
print_info "Additional Information:"
echo "  - To uninstall: ./scripts/uninstall.sh"
echo "  - To reconfigure: yourcloud_client setup"
echo "  - For help: yourcloud_client --help"
echo "  - View status: yourcloud_client status"