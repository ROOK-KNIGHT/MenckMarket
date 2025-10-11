#!/bin/bash
# VolFlow Google SSO Deployment Script
# Sets up and runs the complete Google SSO authentication system

set -e  # Exit on any error

echo "🚀 VolFlow Google SSO Deployment Starting..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    print_error "This script is designed for macOS. Please adapt for your OS."
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "auth_server.py" ]]; then
    print_error "auth_server.py not found. Please run this script from the VolFlow directory."
    exit 1
