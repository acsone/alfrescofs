import fsspec

from .core import (
    AlfrescoBufferedFile,
    AlfrescoFS,
    AlfrescoStreamedFile,
)

# Register AlfrescoFS for all supported protocols
# Use clobber=True to allow re-registration
fsspec.register_implementation("alfd", AlfrescoFS, clobber=True)
