
"""
Utilities to query the Gaia Observation Forecast Tool in a programmatic manner.
"""

import requests
from astropy import units as u
from astropy.table import Table
from collections import OrderedDict
from io import BytesIO


_host = "https://gaia.esac.esa.int"

_default_observing_period_from = "2014-09-26T00:00:00"
_default_observing_period_to = "2019-06-07T00:00:00"

@u.quantity_input(ra=u.deg, dec=u.deg)
def forecast_position(ra, dec, observation_period_from=None,
    observation_period_to=None, full_output=False):
    """
    Submit an asynchronous request to the Gaia forecast tool for the observation
    times of the given sky position.

    :param ra:
        Right ascension [degrees].

    :param dec:
        Declination [degrees].

    :param observation_period_from: [optional]
        The beginning of the observation period to include in UTC ISO format:
        `YYYY-MM-DDTHH:mm:ss`. If `None` is given then it will default to the
        beginning of the observing period allowed (2014-09-26T00:00:00).

    :param observation_period_to: [optional]
        The end of the observation period to include in UTC ISO format.
        `YYYY-MM-DDTHH:mm:ss`. If `None` is given then it will default to the
        end of the allowed observing period (2019-06-07T00:00:00).

    :param full_output: [optional]
        If `True`, return a three-length tuple containing the forecasts, the
        result identifier, and the session used to perform queries.

    :returns:
        An astropy table of forecast observations, and if `full_output` is True,
        the result identifier and the session used to perform queries.
    """

    # TODO: Check observations are in UTC ISO format before submitting the query.
    if observation_period_from is None:
        observation_period_from = _default_observing_period_from

    if observation_period_to is None:
        observation_period_to = _default_observing_period_to

    # We need to do an initial query to GOST, otherwise it sends back
    # "The following error occurred: null" when we try to submit a forecast
    session = requests.Session()
    session.get("{}/gost/".format(_host))

    data = OrderedDict([
        ("serviceCode", 1),
        ("srcname", ""),
        ("inputmode", "single"),
        ("srcra", ra.to(u.deg).value),
        ("srcdec", dec.to(u.deg).value),
        ("from", observation_period_from),
        ("to", observation_period_to),
    ])
    # Force our request to be sent as a multipart
    files = dict(csvfilename="")
    headers = dict([
        ("cache", "false"),
        ("processData", "false"),
    ])

    kwds = dict(data=data, headers=headers, files=files)

    r = session.post("{}/gost/GostServlet".format(_host), **kwds)
    if not r.ok:
        r.raise_for_status()

    # Check for an error.
    assert "error" not in str(r.content), \
        "An error occurred: {}".format(r.content)

    # Parse the request ID and return it.
    result_id = str(r.content).split()[3].split("<")[0]

    # The full identifier requires the session ID *and* the result ID.
    identifier = "{}/{}".format(session.cookies["JSESSIONID"], result_id)

    # TODO: Should we until it is finished?
    fmt = "csv"
    result = session.get("{0}/gost/export.jsp".format(_host), 
                         params=dict(id=identifier, format=fmt))

    forecast = Table.read(BytesIO(result.content), format=fmt)

    # TODO: Rename columns and set units?
    if full_output:
        return (forecast, identifier, session)

    return forecast



if __name__ == "__main__":

    ra = 34.0 * u.deg
    dec = -13.0 * u.deg

    forecast = forecast_position(ra, dec)
