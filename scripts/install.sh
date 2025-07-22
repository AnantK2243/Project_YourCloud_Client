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

BINARY_PATH="target/release/Project_YourCloud_Client"
if [ ! -f "$BINARY_PATH" ]; then
    print_error "Binary not found at $BINARY_PATH after build."
    exit 1
fi

print_success "Binary built successfully!"

# Step 2: Install binary
print_info "Installing binary to /usr/local/bin/Project_YourCloud_Client..."
sudo cp "$BINARY_PATH" /usr/local/bin/Project_YourCloud_Client
sudo chmod 755 /usr/local/bin/Project_YourCloud_Client
print_success "Binary installed successfully!"

# Step 3: Collect configuration from user
echo ""
print_info "Configuration Setup"
echo "==================="

# Get Node ID
while true; do
    read -p "Enter your Node ID (from dashboard): " NODE_ID
    if validate_required "$NODE_ID" "Node ID"; then
        break
    fi
done

# Get Auth Token
while true; do
    read -p "Enter your Auth Token (from dashboard): " AUTH_TOKEN
    if validate_required "$AUTH_TOKEN" "Auth Token"; then
        break
    fi
done

# Get Storage Path
DEFAULT_STORAGE_PATH="$HOME/Project_YourCloud"
prompt_with_default "Enter storage directory path" "$DEFAULT_STORAGE_PATH" STORAGE_PATH

# Get Storage Size
prompt_with_default "Enter maximum storage size (GB)" "40" MAX_STORAGE_GB


# Step 4: Create configuration file
CONFIG_DIR="$HOME/.config/Project_YourCloud"
CONFIG_PATH="$CONFIG_DIR/config.toml"

print_info "Creating configuration file at $CONFIG_PATH..."
mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_PATH" << EOF
node_id = "$NODE_ID"
auth_token = "$AUTH_TOKEN"
storage_path = "$STORAGE_PATH"
max_storage_gb = $MAX_STORAGE_GB
EOF

chmod 600 "$CONFIG_PATH"
print_success "Configuration file created!"

# Step 5: Ask about systemd setup
echo ""
print_info "Service Setup"
echo "============="
read -p "Do you want to set up systemd service for automatic startup? (y/n) [y]: " SETUP_SYSTEMD
SETUP_SYSTEMD=${SETUP_SYSTEMD:-y}

if [[ "$SETUP_SYSTEMD" =~ ^[Yy]$ ]]; then
    # Create systemd service file
    SYSTEMD_SERVICE_PATH="/etc/systemd/system/project-yourcloud-storage.service"
    
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
ExecStart=/usr/local/bin/Project_YourCloud_Client
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
    sudo systemctl enable project-yourcloud-storage.service
    
    print_success "Systemd service created and enabled!"
    
    # Ask if user wants to start the service now
    read -p "Do you want to start the service now? (y/n) [y]: " START_NOW
    START_NOW=${START_NOW:-y}
    
    if [[ "$START_NOW" =~ ^[Yy]$ ]]; then
        sudo systemctl start project-yourcloud-storage.service
        print_success "Service started!"
        print_info "You can check the service status with: sudo systemctl status project-yourcloud-storage"
        print_info "View logs with: sudo journalctl -u project-yourcloud-storage -f"
    else
        print_info "You can start the service later with: sudo systemctl start project-yourcloud-storage"
    fi
else
    print_info "Systemd service not configured. You can run the storage node manually."
fi

# Step 6: Display final information
echo ""
print_success "Installation completed successfully!"
echo "===================================="
echo ""
print_info "Configuration file: $CONFIG_PATH"
print_info "Storage directory: $STORAGE_PATH"
print_info "Binary location: /usr/local/bin/Project_YourCloud_Client"
echo ""

if [[ "$SETUP_SYSTEMD" =~ ^[Yy]$ ]]; then
    print_info "Systemd Commands:"
    echo "  Start service:   sudo systemctl start project-yourcloud-storage"
    echo "  Stop service:    sudo systemctl stop project-yourcloud-storage"
    echo "  Restart service: sudo systemctl restart project-yourcloud-storage"
    echo "  Check status:    sudo systemctl status project-yourcloud-storage"
    echo "  View logs:       sudo journalctl -u project-yourcloud-storage -f"
fi

echo ""
print_info "Your storage node is ready! Check your dashboard to verify the connection."