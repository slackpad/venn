# Venn

Venn is a simple tool for working with large sets of files with potentially many duplicates. It was created after I mistakenly added 65k bad photos to my Apple Photos app and left it trying to delete them for days with no luck. I decided I wanted a tool that I understood to try to recover from this and let me clean up a ton of duplicates in various backups.

WARNING! This tool is hacky and only lightly tested - use at your own risk. See the [LICENSE](LICENSE) for more details.

## How It Works

Venn uses a single database file for all its work, and allows you to crawl trees of files and index them. You can then use set operations to combine these indexes in various ways, and then you can materialize them into a standard tree structure. The materialized tree is managed in a content addressable fashion and naturally avoids duplication.

Here's an example:

```
# Initialize Venn
venn init

# Scan a Google Photos Takeout and add to "google" index; this will preserve
# the "photo taken" timestamps as well as the metadata JSON files in the
# materialized view.
venn index add-google-photos-takeout google MyBackup

# Scan all of MyPhotos folder and add to "photos" index
venn index add-files photos MyPhotos

# Scan all of WrongOnes folder and add to "bad_import" index
venn index add-files bad_import WrongOnes

# Make a new index with all of the bad import taken out
venn set difference cleaned_up photos bad_import

# Materialize all of the cleaned up photos into MyNewPhotoLibrary folder
venn index materialize cleaned_up MyNewPhotoLibrary
```

There are additional commands to perform set unions, and to manage indexes. Run `venn` with no arguments for help.