#!/bin/bash
# quick-fix-tests.sh - Quick fix for pytest testing framework deployment issues

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🔧 Quick Fix for Pytest Testing Framework${NC}"
echo "=================================="

# Check if we're in the right directory
if [ ! -f "src/requirements.txt" ]; then
    echo -e "${RED}❌ Must be run from project root directory${NC}"
    echo -e "${YELLOW}Expected structure: ./src/requirements.txt${NC}"
    exit 1
fi

# 1. Fix requirements.txt
echo -e "\n${BLUE}📦 Step 1: Fixing requirements.txt${NC}"
if ! grep -q "pytest" src/requirements.txt; then
    echo -e "${YELLOW}Adding pytest dependencies...${NC}"
    cat >> src/requirements.txt << 'EOF'

# Testing dependencies (required for test mode in Cloud Functions)
pytest>=7.0.0
pytest-json-report>=1.5.0
EOF
    echo -e "${GREEN}✅ Added pytest dependencies${NC}"
else
    echo -e "${GREEN}✅ pytest already in requirements.txt${NC}"
fi

# 2. Create proper .gcloudignore
echo -e "\n${BLUE}📁 Step 2: Creating .gcloudignore${NC}"
cat > src/.gcloudignore << 'EOF'
# Python bytecode
__pycache__/
*.py[cod]
*$py.class

# Virtual environments  
env/
venv/
.venv/

# IDE files
.vscode/
.idea/

# OS files
.DS_Store

# Git
.git/

# Local development
.env
main.py

# Test results (but keep tests/ directory)
.pytest_cache/
test-results/
comprehensive-test-results.json

# Excel import (not needed)
excel_import/

# Development scripts
invoke_cloud_function.py
test-ingest.sh
validate-framework.sh
EOF

echo -e "${GREEN}✅ Created .gcloudignore (tests/ will be included)${NC}"

# 3. Verify test structure
echo -e "\n${BLUE}🔍 Step 3: Verifying test structure${NC}"
required_files=(
    "src/tests/__init__.py"
    "src/tests/conftest.py" 
    "src/tests/test_framework_validation.py"
    "src/tests/fixtures/__init__.py"
    "src/tests/fixtures/test_session.py"
)

missing_files=()
for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}  ✅ $file${NC}"
    else
        echo -e "${RED}  ❌ $file${NC}"
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -gt 0 ]; then
    echo -e "${RED}❌ Missing critical test files${NC}"
    exit 1
fi

# 4. Test local import
echo -e "\n${BLUE}🧪 Step 4: Testing local import${NC}"
cd src
python3 -c "
import sys
sys.path.insert(0, '.')
try:
    from tests import run_production_tests
    print('✅ Test framework import successful')
except ImportError as e:
    print(f'❌ Import failed: {e}')
    sys.exit(1)
" && echo -e "${GREEN}✅ Local import test passed${NC}" || echo -e "${RED}❌ Local import test failed${NC}"

cd ..

# 5. Deploy instruction
echo -e "\n${BLUE}🚀 Step 5: Ready for deployment${NC}"
echo -e "${GREEN}Now run:${NC}"
echo -e "  ./deploy.sh ingest dev"
echo -e ""
echo -e "${GREEN}Then test with:${NC}"
echo -e "  curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \\"
echo -e "    -H 'Content-Type: application/json' \\"
echo -e "    -d '{\"mode\": \"test\", \"test_type\": \"infrastructure\"}'"
echo -e ""
echo -e "${YELLOW}If tests still fail after deployment, check:${NC}"
echo -e "  1. Cloud Function logs: gcloud functions logs read hubspot-ingest-dev --region=europe-west1"
echo -e "  2. Deployment package contents"
echo -e "  3. Import paths in Cloud Functions environment"
echo -e ""
echo -e "${BLUE}✅ Quick fix completed!${NC}"