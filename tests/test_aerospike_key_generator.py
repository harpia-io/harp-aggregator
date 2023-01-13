notifications = {
    1: {
        'ui': {
            'active': {
                'notifications': [
                    {'test': 'rest'},
                    {'test2': 'rest2'}
                ],
                'statistics': 0
            }


        }
    }
}


def flatten_dict(pyobj, keystring=''):
    if type(pyobj) == dict:
        keystring = keystring + '___' if keystring else keystring
        for k in pyobj:
            yield from flatten_dict(pyobj[k], keystring + str(k))
    else:
        yield keystring, pyobj


print({k: v for k, v in flatten_dict(notifications)})
