YTBHNRTS: Yet to be humorously named R-tree server
==================================================

A simple to use Solr-ish spatial search server written in Python. Items are
indexed and searched using a JSON and HTTP POST/GET interface. Indexes are
persistant, stored on disk, and can be manipulated by command line
administrative tools. Like Solr, security will be left up to the service
container (http://wiki.apache.org/solr/SolrSecurity). HTTP cache control will
be used properly so one can profit from Varnish or other proxies.

On one end of the service will be Tornado (http://www.tornadoweb.org/), because
a server using an event loop is a good match for the R-tree backend
(http://pypi.python.org/pypi/Rtree/), and because it's more popular than
Twisted.

Usage
=====

Let's begin with operational, non-administrative usage.

Index discovery
---------------

An index is a web resource with a bookmarkable URI. Its HTML representation
conveys how-to information, basic metadata, and actionable links to users. It
describes available queries and explains the spatial extent of its contents.

(Should an index provide a paged GeoRSS feed or paged JSON response of all its
items?)

Index query
-----------

An index query is a web resource with a bookmarkable URI. Accessed via HTTP
GET, the JSON representation lists metadata for matching items:

  {... 'items': [{'id': '1', 'bounds': (minx, miny, maxx, maxy), ...}]}

Item metadata includes: 1) a globally unique 'id', 2) bounds (minx, miny, maxx,
maxy), 3) geometry (GeoJSON), and 4) a 'properties' mapping which could contain
just about anything.

(Note: A GeoRSS/OpenSearch formatted response would be directly viewable in
Google Maps, etc.)

Default queries: bounding box intersections and nearest neighbors.

Item append
-----------

One or more items can be appended to an index by POST of JSON data:

  POST /example-index
  
  {'method': 'append', 'items': [{'id': ...}] }

Batch append is all or nothing. 200 "ok" or a 40x. Requirement of unique ids
guards against double post. Use 409 "conflict".

Item removal
------------

One or more items can be removed from an index 

  POST /example-index
  
  {'method': 'remove', 'items': [{'id': ...}] }

In this case, only ids need be provided for the items. Again, this is all or
nothing.

Administrative usage is via shell/terminal program(s) that will have a much
better name than "ytbhnrts-admin". (TODO: web admin interface)

Configuration
-------------

Logging, etc, configured through an ini-style config file.

Create index
------------

Create a new r-tree with its various resources via "ytbhnrts-admin create ..."

Delete index
------------

"ytbhnrts-admin delete ..."

Dump index
----------

Export items to JSON or XML via "ytbhnrts-admin dump ..."

Reindexing
----------

Query performance can be improved by reprocessing persisted items. Time is
order of 10 minutes, during which no other access is allowed (TODO: reindex to
a copy and swap within the server process with no downtime?)

Manage queries (maybe)
----------------------

Create queries in addition to the default intersection and nearest?

Undo
----

Snapshot the persisted data to allow limited reversal of appends and removes.

