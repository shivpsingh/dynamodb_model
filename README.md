# dynamodb_model

Usage:

```python
import os
from ddbmodel import model

class SampleModel(model.Model):
    email = model.Column(model.PRIMARY_KEY)
    username = model.Column(model.PARTITION_KEY)
    name = model.Column()
    details = model.Column()

    DDBTableName = 'sample_dynamo_db_table'
    AWSRegion = os.getenv('AWS_REGION')
```

```python
data = SampleModel()
data.email = "sampleemail@example.com"
data.username = "sampleuser"
data.name = "Some Name"
data.details = {
    "name": "FirstName LastName",
    "city": "India"
}

# For all the operations atleast partition key is set
# Example
data = SampleModel()
data.email = "sampleemail@example.com"

# Above two lines should be written to perform any below actions.

# Save Details to DB
data.save()

# Update Name
data.name = "New Name"

# Push Updated details to DB
data.update_row()

# Fetch details - Python Dict
# This will not update any variables of data object
response = data.fetch_row()

# Fetch and update columns
# This is will update variables of data object too.
data.fetch_and_populate_cols()

# Delete a row
data.delete_row()
```
