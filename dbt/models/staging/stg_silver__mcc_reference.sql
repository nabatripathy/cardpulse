select mcc, mcc_description, category_group
from {{ source('silver', 'mcc_reference') }}
