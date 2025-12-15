#!/usr/bin/env python3
"""
Quick test script to verify backend endpoints are working
"""
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def test_health():
    """Test health check endpoint"""
    print("Testing health check...")
    try:
        res = requests.get(f"{BASE_URL}/health")
        print(f"✅ Health: {res.status_code} - {res.json()}")
        return True
    except Exception as e:
        print(f"❌ Health failed: {e}")
        return False

def test_revenue():
    """Test revenue endpoint"""
    print("\nTesting revenue endpoint...")
    try:
        res = requests.get(f"{BASE_URL}/api/analytics/revenue")
        data = res.json()
        print(f"✅ Revenue: {res.status_code}")
        if data:
            print(f"   Sample: {json.dumps(data[0], indent=2)}")
            print(f"   Total items: {len(data)}")
        else:
            print("   ⚠️  Empty response")
        return True
    except Exception as e:
        print(f"❌ Revenue failed: {e}")
        return False

def test_items_by_level():
    """Test items by level endpoint"""
    print("\nTesting items-by-level endpoint...")
    try:
        res = requests.get(f"{BASE_URL}/api/analytics/items-by-level")
        data = res.json()
        print(f"✅ Items by level: {res.status_code}")
        if data:
            print(f"   Sample: {json.dumps(data[0], indent=2)}")
            print(f"   Total levels: {len(data)}")
        else:
            print("   ⚠️  Empty response")
        return True
    except Exception as e:
        print(f"❌ Items by level failed: {e}")
        return False

def test_items_detail_by_level():
    """Test items detail by level endpoint"""
    print("\nTesting items-by-level/5 endpoint...")
    try:
        res = requests.get(f"{BASE_URL}/api/analytics/items-by-level/5")
        data = res.json()
        print(f"✅ Items detail (Level 5): {res.status_code}")
        if data:
            print(f"   Sample: {json.dumps(data[0], indent=2)}")
            print(f"   Total items: {len(data)}")
        else:
            print("   ⚠️  Empty response")
        return True
    except Exception as e:
        print(f"❌ Items detail failed: {e}")
        return False

def test_fail_rate():
    """Test fail rate endpoint"""
    print("\nTesting fail-rate endpoint...")
    try:
        res = requests.get(f"{BASE_URL}/api/analytics/fail-rate")
        data = res.json()
        print(f"✅ Fail rate: {res.status_code}")
        if data:
            print(f"   Sample: {json.dumps(data[0], indent=2)}")
            print(f"   Total items: {len(data)}")
        else:
            print("   ⚠️  Empty response")
        return True
    except Exception as e:
        print(f"❌ Fail rate failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Backend Integration Test")
    print("=" * 60)
    
    results = []
    results.append(("Health", test_health()))
    results.append(("Revenue", test_revenue()))
    results.append(("Items by Level", test_items_by_level()))
    results.append(("Items Detail", test_items_detail_by_level()))
    results.append(("Fail Rate", test_fail_rate()))
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(p for _, p in results)
    if all_passed:
        print("\n✅ All tests passed! Integration ready.")
    else:
        print("\n❌ Some tests failed. Check backend and database.")
