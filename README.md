# file_metadata_comparison

This file connects withe postgresql and creates an empty table in that db and when a new file is given, then it reads and extracts the metadata of the file and stores the metadata in the table and if another file with same columns is given for reading then it compares the metadata of new file with the old file and gives the changes and stores in  the db.
