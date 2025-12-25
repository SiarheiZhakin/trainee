[
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d',
                    'date': '$at'
                }
            },
            'average_rate': {
                '$avg': '$score'
            }
        }
    }, {
        '$addFields': {
            'average_rate_round': {
                '$round': [
                    '$average_rate', 1
                ]
            }
        }
    }
]