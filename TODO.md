# TODO

* Add support for Google Takeouts:
    * Skip all the .json files in the index in this mode.
    * Use the .json for all other files (if present) to set the metadata (at least the time stamp).
    * Make a new indexer "add-takeout".
    * Capture the time stamp in the index and use that when materializing.