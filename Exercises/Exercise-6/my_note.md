# Notes on Ex 6

Had an issue reading data from compressed state (.zip), despite documentation indicating that it should be supported:

- Issue Observed: Files were read but data returned appeared encoded
- Solutions tried:
  - Tried reading individual files directly, as zipped csv, into spark dataframe
  - Same as above but reading directory
  - Same as above but into spark rdd(s). both individual files, and directory
  - Tried opening zip at runtime to load into rdd
  - Tried various read options regarding header, inferring schema, encoding, etc
  - Tried to unzip the file and re-zip using gzip, since this seems to be fully supported as noted in documentation
- Outcomes of above:
  - Opening the zip at runtime and passing to spark gave out of memory errors
  - Also ran into error regarding inability to find file footer. Most resolutions on line indicate not having a properly setup cluster
- Final Solution:
  - Gave up on reading from compressed state and just unzipped and proceeded with the exercise
