#!/bin/bash
# Setup script for IPM SQL Dashboard

echo "==========================================="
echo "IPM Dashboard - SQL Setup"
echo "==========================================="

# Check Python version
python3 --version

# Install dependencies
echo ""
echo "Installing Python packages..."
pip3 install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Restore database: mysql < backup.sql"
echo "2. Edit ipm_sql_dashboard.py with this month's data"
echo "3. Run: python3 ipm_sql_dashboard.py"
echo ""
echo "==========================================="
