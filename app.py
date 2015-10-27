


import logging
import datetime
import os
import time
import urllib
import urllib2
from cStringIO import StringIO
import gzip
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template
from google.appengine.api.urlfetch import fetch
from google.appengine.api import mail

try:
  from urlparse import parse_qs # python 2.6
except ImportError:
  from cgi import parse_qs # python 2.5

from django.utils import simplejson as json
import conf

class Redirect(Exception):
  def __init__(self, _url):
    self._url = _url

  def url(self):
    return self._url

class APIError(Exception):
  pass

class Handler(webapp.RequestHandler):
  def base_url(self):
    return 'http://'+os.environ['HTTP_HOST']+'/'

  # The URL for accessing the Facebook REST API
  def rest_api_url(self, access_token, method, parameters={}):
    parameters['access_token'] = access_token
    parameters['method'] = method
    parameters['format'] = 'JSON'
    return conf.API_BASE_URL + urllib.urlencode(parameters)

  # Make a call to the Facebook REST API
  def rest_api_call(self, access_token, method, parameters={}):
    url = self.rest_api_url(access_token, method, parameters)
    response = fetch(url)
    if response.status_code != 200:
      raise APIError(str(response.status_code)+': '+url+'\n'+response.content)
    return json.loads(response.content)

  # The URL for accessing the Facebook Graph API
  def graph_api_url(self, access_token, oid, edge=None, parameters={}):
    if access_token:
      parameters['access_token'] = access_token

    return (conf.GRAPH_BASE_URL +
        (str(oid) if oid else '') +
        ('/'+str(edge) if edge else '') +
        '?' + urllib.urlencode(parameters))

  # Make a call to the Facebook Graph API
  def graph_api_call(self, access_token, oid, edge=None, parameters={}):
    url = self.graph_api_url(access_token, oid, edge, parameters)
    response = fetch(url)
    if response.status_code != 200:
      raise APIError(str(response.status_code)+': '+url+'\n'+response.content)
    return json.loads(response.content)

  # Get a valid OAuth 2.0 access token, redirecting to the Facebook OAuth
  # endpoints if necessary
  def access_token(self):
    if self.request.get('access_token'):
      return self.request.get('access_token')

    if not self.request.get('code'): # No code
      raise Redirect(self.graph_api_url(
          None, 'oauth', 'authorize',
          {'client_id': conf.APP_ID,
           'redirect_uri': self.base_url(),
           'scope': 'manage_pages,read_insights'}))

    response = fetch(self.graph_api_url(
        None, 'oauth', 'access_token',
        {'client_id': conf.APP_ID,
         'redirect_uri': self.base_url(),
         'client_secret': conf.APP_SECRET,
         'code': self.request.get('code')}))
    if response.status_code != 200: # Expired code
      raise Redirect(self.base_url())
    return parse_qs(response.content)['access_token'][0]

  def index(self, access_token):
    # Get the current user
    user = self.rest_api_call(access_token, 'users.getLoggedInUser')

    # Get the pages and apps the current user owns
    pages = map(
      lambda x: int(x['page_id']),
      self.rest_api_call(
        access_token, 'fql.query',
        {'query':
           'SELECT page_id FROM page_admin WHERE uid=%d' % user}))
    apps = map(
      lambda x: int(x['application_id']),
      self.rest_api_call(
        access_token, 'fql.query',
        {'query':
           'SELECT application_id FROM developer WHERE developer_id=%d' % user}))
    pages = set(pages) | set(apps)

    # Get page and app profiles
    page_data = self.graph_api_call(access_token, None, None, {'ids': ','.join(map(str, pages))})

    self.response.out.write(template.render('index.html', {'access_token': access_token, 'pages': page_data}))

  def GetPeriod(self, access_token, oid):
    insights = self.graph_api_call(access_token, oid, 'insights', {'period': 0, 'metric': 'page_fans'})
    start_date = insights['data'][0]['values'][0]['end_time']
    while insights['data']:
      end_date = insights['data'][0]['values'][-1]['end_time']
      insights = json.loads(urllib2.urlopen(insights['paging']['next']).read())
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S+0000').date()
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S+0000').date()
    return start_date, end_date

  def download(self, access_token):
    oid = self.request.get('id')
    page_data = self.graph_api_call(access_token, None, None, {'ids': str(oid)})
    name, url = page_data[oid]['name'], page_data[oid]['link']

    # Get Insights data
    # insights = self.graph_api_call(access_token, oid, 'insights', {'since': date, 'until': date+datetime.timedelta(1)})
    start_date, end_date = self.GetPeriod(access_token, oid)
    today = datetime.datetime.now().date()

    # Prepare output
    result = ['name,%s,url,%s,access_token,%s' % (name, url, access_token), 'object_id,metric,end_time,period,value']
    insights = self.graph_api_call(access_token, oid, 'insights', {'period': 86400 , 'end_time': start_date})
    temp_date = datetime.datetime.strptime(insights['data'][0]['values'][-1]['end_time'], '%Y-%m-%dT%H:%M:%S+0000').date()
    first_iteration = True
    while first_iteration or temp_date <= today:
      first_iteration = False
      for metric in insights['data']:
        for row in metric['values']:
          date = datetime.datetime.strptime(row['end_time'], '%Y-%m-%dT%H:%M:%S+0000').date() + datetime.timedelta(-1)
          result.append('%s,%s,%s,%s,%s' % (metric['id'].partition('/')[0], metric['name'], date,metric['period'], row['value']))
      insights = json.loads(urllib2.urlopen(insights['paging']['next']).read())
      temp_date = datetime.datetime.strptime(insights['data'][0]['values'][-1]['end_time'], '%Y-%m-%dT%H:%M:%S+0000').date()

    buffer = StringIO()
    temp_file = gzip.GzipFile(mode='wb', fileobj=buffer)
    temp_file.write('\n'.join(result))
    temp_file.close()
    result = buffer.getvalue()
    buffer.close()

    # Mail result
    mail.send_mail(sender='The Non-Profit Study <christopherandrewbail+the_non_profit_study@gmail.com>',
                                to='appsubmission+the_non_profit_study@findyourpeople.org',
                                subject='Insights for %s' % (name), body='', attachments=[('%s.csv.gz' % name, result)])

    # Format as CSV and output
    #self.response.headers['Content-Type'] = 'text/csv' if not conf.DEBUG else 'text/plain'
    #self.response.headers['Content-Disposition'] = 'attachment; filename="%s.csv.gz"' % (name)
    #self.response.out.write(result)

    self.redirect("http://www.findyourpeople.org/p/loading.html")

  # Handle GET requests
  def get(self):
    try:
      access_token = self.access_token()

      if (self.request.get('id')):
        self.download(access_token)
      else:
        self.index(access_token)

    except Redirect, r:
      self.redirect(r.url())
    except APIError, e:
      if conf.DEBUG:
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('Error Retrieving Data\n')
        self.response.out.write(str(e))


application = webapp.WSGIApplication([('/', Handler)], debug=True)

def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
