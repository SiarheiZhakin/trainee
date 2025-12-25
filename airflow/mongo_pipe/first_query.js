[
    {
        '$group': {
            '_id': '$content',
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 5
    }
]