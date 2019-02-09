# Import SyncTK using Python .NET
import clr
clr.AddReference("SyncTK")
from SyncTK import *

# Basic module properties
name = "synctk"
__all__ = ["synctk"]