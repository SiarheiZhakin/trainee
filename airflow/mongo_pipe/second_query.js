[
    {
        '$group': {
            '_id': '$content',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$project': {
            'length': {
                '$strLenCP': {
                    '$toString': '$_id'
                }
            }
        }
    }, {
        '$match': {
            'length': {
                '$lt': 5
            }
        }
    }
]