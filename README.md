# s3QueryOptimizer

currently filtering on the following:
<br />
only looking at physical plan
<br />
1. inside physical plan scanning taking every step that has "s3a" in it
<br />
2. filter for only parquet formats
<br />
3. removing everything with insert in it
<br /> 
 