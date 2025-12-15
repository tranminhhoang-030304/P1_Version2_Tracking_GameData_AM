# Re-export Item from __init__.py for backward compatibility
from backend.models import Item

__all__ = ["Item"]
