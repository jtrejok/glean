# glean
There should be a directory `data`  in the same location containing the files `line_item.csv` and `invoice.csv`
With pyspark installed run
```
python glean_log.py 
```
and a csv file `gleans.csv` will be produced.
It will look for the input files as `"data/invoice.csv"` and `"data/line_item.csv"`
