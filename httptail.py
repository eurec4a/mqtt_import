import time
import requests

def http_iter(url, throttle=5, linebreak=None):
    """
    Iterate over chunks of a document which is accessible via HTTP.

    The HTTP server should support bytewise Range requests.
    Furthermore, `http_iter` assumes that the document is only appended but
    never modified otherwise.
    """
    pos = 0
    last_request_time = 0
    remainder = b''
    while True:
        headers = {"Range": "bytes={}-".format(pos)}
        dt = time.time() - last_request_time
        if dt < throttle:
            time.sleep(max(0, throttle - dt))
        res = requests.get(url, headers=headers)
        last_request_time = time.time()
        if res.status_code == requests.codes.ok:
            if pos != 0:
                print("WARNING: server does not support range requests")
            new_data = res.content[pos:]
        elif res.status_code == requests.codes.partial_content:
            new_data = res.content
        elif res.status_code == requests.codes.requested_range_not_satisfiable:
            continue
        else:
            res.raise_for_status()
            break
        pos += len(new_data)
        if linebreak is not None:
            lines = new_data.split(linebreak)
            lines[0] = remainder + lines[0]
            remainder = lines[-1]
            lines = lines[:-1]
            for line in lines:
                yield line
        else:
            yield new_data
