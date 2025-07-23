#!/bin/bash

set -e  # Exit on any error

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

# Function to ask for confirmation
confirm() {
    local prompt="$1"
    local default="$2"
    
    if [ "$default" = "y" ]; then
        read -p "$prompt [Y/n]: " response
        response=${response:-y}
    else
        read -p "$prompt [y/N]: " response
        response=${response:-n}
    fi
    
    [[ "$response" =~ ^[Yy]$ ]]
}

# Function to safely remove file/directory
safe_remove() {
    local path="$1"
    local description="$2"
    
    if [ -e "$path" ]; then
        if confirm "Remove $description at $path?" "n"; then
            if [ -d "$path" ]; then
                rm -rf "$path"
            else
                rm -f "$path"
            fi
            print_success "$description removed: $path"
        else
            print_info "Keeping $description: $path"
        fi
    else
        print_info "$description not found: $path"
    fi
}

print_warning "Project YourCloud Storage Node Uninstaller"
echo "==========================================="
echo ""
print_warning "This script will remove the Project YourCloud storage node from your system."
echo ""

if ! confirm "Are you sure you want to continue with uninstallation?" "n"; then
    print_info "Uninstallation cancelled."
    exit 0
fi

echo ""

# Step 1: Stop and disable systemd service
SYSTEMD_SERVICE_PATH="/etc/systemd/system/yourcloud-storage.service"

if [ -f "$SYSTEMD_SERVICE_PATH" ]; then
    print_info "Found systemd service. Stopping and disabling..."
    
    # Stop the service if it's running
    if systemctl is-active --quiet yourcloud-storage.service; then
        print_info "Stopping yourcloud-storage service..."
        sudo systemctl stop yourcloud-storage.service
        print_success "Service stopped."
    fi
    
    # Disable the service if it's enabled
    if systemctl is-enabled --quiet yourcloud-storage.service; then
        print_info "Disabling yourcloud-storage service..."
        sudo systemctl disable yourcloud-storage.service
        print_success "Service disabled."
    fi
    
    # Remove service file
    if confirm "Remove systemd service file?" "y"; then
        sudo rm -f "$SYSTEMD_SERVICE_PATH"
        sudo systemctl daemon-reload
        print_success "Systemd service file removed and daemon reloaded."
    fi
else
    print_info "No systemd service found."
fi

echo ""

# Step 2: Remove binary
BINARY_PATH="/usr/local/bin/yourcloud_client"
if [ -f "$BINARY_PATH" ]; then
    if confirm "Remove binary at $BINARY_PATH?" "y"; then
        sudo rm -f "$BINARY_PATH"
        print_success "Binary removed: $BINARY_PATH"
    fi
else
    print_info "Binary not found: $BINARY_PATH"
fi

echo ""

# Step 3: Storage data (before removing config files)
print_info "Storage Data Management"
echo "======================="

# Function to analyze storage directory
analyze_storage() {
    local storage_path="$1"
    local file_count=0
    local total_size=0
    
    if [ -d "$storage_path" ]; then
        file_count=$(find "$storage_path" -type f 2>/dev/null | wc -l)
        if command -v du >/dev/null 2>&1; then
            total_size=$(du -sb "$storage_path" 2>/dev/null | cut -f1)
        fi
    fi
    
    echo "$file_count:$total_size"
}

# Function to format file size
format_size() {
    local bytes="$1"
    if [ "$bytes" -lt 1024 ]; then
        echo "${bytes} bytes"
    elif [ "$bytes" -lt 1048576 ]; then
        echo "$((bytes / 1024)) KB"
    elif [ "$bytes" -lt 1073741824 ]; then
        echo "$((bytes / 1048576)) MB"
    else
        echo "$((bytes / 1073741824)) GB"
    fi
}

# Function to find and handle storage directories
handle_storage_data() {
    local config_file="$1"
    
    if [ -f "$config_file" ] && [ -r "$config_file" ]; then
        # Extract storage path from config
        local storage_path=$(grep '^storage_path = ' "$config_file" 2>/dev/null | sed 's/storage_path = "\([^"]*\)"/\1/' | tr -d '"' | head -1)
        
        if [ -n "$storage_path" ] && [ -d "$storage_path" ]; then
            print_warning "Found storage directory: $storage_path"
            
            # Analyze storage directory
            local analysis=$(analyze_storage "$storage_path")
            local file_count=$(echo "$analysis" | cut -d: -f1)
            local total_size=$(echo "$analysis" | cut -d: -f2)
            
            if [ "$file_count" -gt 0 ]; then
                print_warning "Storage directory contains $file_count files ($(format_size $total_size))."
                echo ""
                print_warning "⚠️  WARNING: This will permanently delete all stored data! ⚠️"
                print_warning "This includes all chunks and files that may have been stored on this node."
                print_warning "Once deleted, this data cannot be recovered and will cause data loss."
                print_warning "Consider backing up important data before proceeding."
                echo ""
                
                if confirm "Delete storage directory and ALL stored data?" "n"; then
                    print_info "Removing storage directory and all data..."
                    # Double confirmation for large amounts of data
                    if [ "$total_size" -gt 1073741824 ]; then  # > 1GB
                        print_warning "This directory contains $(format_size $total_size) of data."
                        if ! confirm "Are you absolutely sure you want to delete this large amount of data?" "n"; then
                            print_info "Storage directory preserved: $storage_path"
                            return
                        fi
                    fi
                    rm -rf "$storage_path"
                    print_success "Storage directory and all data removed: $storage_path"
                else
                    print_info "Storage directory preserved: $storage_path"
                    print_info "You can manually remove it later if needed."
                fi
            else
                print_info "Storage directory is empty."
                if confirm "Remove empty storage directory?" "y"; then
                    rm -rf "$storage_path"
                    print_success "Empty storage directory removed: $storage_path"
                fi
            fi
        fi
    fi
}

# Check for user config and handle storage data first
USER_CONFIG_DIR="$HOME/.config/Project_YourCloud"
USER_CONFIG_FILE="$USER_CONFIG_DIR/config.toml"
SYSTEM_CONFIG_FILE="/etc/Project_YourCloud/config.toml"

# Handle storage data from config files (before deleting them)
if [ -f "$USER_CONFIG_FILE" ]; then
    handle_storage_data "$USER_CONFIG_FILE"
elif [ -f "$SYSTEM_CONFIG_FILE" ]; then
    handle_storage_data "$SYSTEM_CONFIG_FILE"
else
    # Check common default locations if no config files found
    DEFAULT_STORAGE="$HOME/Project_YourCloud"
    if [ -d "$DEFAULT_STORAGE" ]; then
        print_info "Found default storage directory: $DEFAULT_STORAGE"
        
        local analysis=$(analyze_storage "$DEFAULT_STORAGE")
        local file_count=$(echo "$analysis" | cut -d: -f1)
        local total_size=$(echo "$analysis" | cut -d: -f2)
        
        if [ "$file_count" -gt 0 ]; then
            print_warning "Default storage directory contains $file_count files ($(format_size $total_size))."
            print_warning "⚠️  WARNING: This will permanently delete all stored data! ⚠️"
            
            if confirm "Delete default storage directory and ALL stored data?" "n"; then
                rm -rf "$DEFAULT_STORAGE"
                print_success "Default storage directory and all data removed: $DEFAULT_STORAGE"
            else
                print_info "Default storage directory preserved: $DEFAULT_STORAGE"
            fi
        else
            if confirm "Remove empty default storage directory?" "y"; then
                rm -rf "$DEFAULT_STORAGE"
                print_success "Empty default storage directory removed: $DEFAULT_STORAGE"
            fi
        fi
    else
        print_info "No default storage directory found."
    fi
fi

echo ""

# Step 4: Configuration files
print_info "Configuration File Management"
echo "============================="

# Check for user config
USER_CONFIG_DIR="$HOME/.config/Project_YourCloud"
USER_CONFIG_FILE="$USER_CONFIG_DIR/config.toml"

if [ -f "$USER_CONFIG_FILE" ]; then
    print_info "Found user configuration file: $USER_CONFIG_FILE"
    
    safe_remove "$USER_CONFIG_FILE" "configuration file"
    
    # Remove config directory if empty
    if [ -d "$USER_CONFIG_DIR" ] && [ -z "$(ls -A "$USER_CONFIG_DIR" 2>/dev/null)" ]; then
        safe_remove "$USER_CONFIG_DIR" "configuration directory"
    fi
else
    print_info "No user configuration file found."
fi

# Check for system config
SYSTEM_CONFIG_FILE="/etc/Project_YourCloud/config.toml"
if [ -f "$SYSTEM_CONFIG_FILE" ]; then
    safe_remove "$SYSTEM_CONFIG_FILE" "system configuration file"
    
    # Remove system config directory if empty
    SYSTEM_CONFIG_DIR="/etc/Project_YourCloud"
    if [ -d "$SYSTEM_CONFIG_DIR" ] && [ -z "$(ls -A "$SYSTEM_CONFIG_DIR" 2>/dev/null)" ]; then
        if confirm "Remove system configuration directory at $SYSTEM_CONFIG_DIR?" "y"; then
            sudo rm -rf "$SYSTEM_CONFIG_DIR"
            print_success "System configuration directory removed: $SYSTEM_CONFIG_DIR"
        fi
    fi
fi

echo ""

# Step 5: Logs and cache
print_info "Cleanup System Logs and Cache"
echo "=============================="

# Check for systemd logs
if command -v journalctl >/dev/null 2>&1; then
    if journalctl --unit=yourcloud-storage.service --lines=1 >/dev/null 2>&1; then
        if confirm "Clear systemd service logs?" "y"; then
            sudo journalctl --vacuum-time=1s --unit=yourcloud-storage.service >/dev/null 2>&1 || true
            print_success "Systemd service logs cleared."
        fi
    fi
fi

# Check for any remaining process
print_info "Checking for running processes..."
if pgrep -f "yourcloud_client\|Project_YourCloud_Client" >/dev/null 2>&1; then
    print_warning "Found running yourcloud_client processes."
    if confirm "Kill all running yourcloud_client processes?" "y"; then
        pkill -f "yourcloud_client\|Project_YourCloud_Client" || true
        print_success "Running processes terminated."
    fi
fi

echo ""

# Step 6: Final verification
print_info "Uninstallation Summary"
echo "====================="

# Check what remains
remaining_items=()

[ -f "/usr/local/bin/yourcloud_client" ] && remaining_items+=("Binary: /usr/local/bin/yourcloud_client")
[ -f "/etc/systemd/system/yourcloud-storage.service" ] && remaining_items+=("Systemd service: /etc/systemd/system/yourcloud-storage.service")
[ -f "$USER_CONFIG_FILE" ] && remaining_items+=("User config: $USER_CONFIG_FILE")
[ -d "$USER_CONFIG_DIR" ] && remaining_items+=("Config directory: $USER_CONFIG_DIR")
[ -f "$SYSTEM_CONFIG_FILE" ] && remaining_items+=("System config: $SYSTEM_CONFIG_FILE")
[ -d "/etc/Project_YourCloud" ] && remaining_items+=("System config directory: /etc/Project_YourCloud")

# Check for storage directories
for potential_storage in "$HOME/Project_YourCloud" "/var/lib/Project_YourCloud" "/opt/Project_YourCloud"; do
    [ -d "$potential_storage" ] && remaining_items+=("Potential storage: $potential_storage")
done

if [ ${#remaining_items[@]} -eq 0 ]; then
    print_success "Complete uninstallation successful!"
    print_success "All Project YourCloud components have been removed from your system."
else
    print_warning "Some items were not removed:"
    for item in "${remaining_items[@]}"; do
        echo "    - $item"
    done
    echo ""
    print_info "You can manually remove these items if desired."
fi

echo ""
print_info "Uninstallation completed."
print_info "Thank you for using Project YourCloud!"
