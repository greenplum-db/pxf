/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "pxfbridge.h"
#include "pxfheaders.h"

#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"

/* helper function declarations */
static void build_uri_for_read(gphadoop_context *context);
static void build_uri_for_write(gphadoop_context *context);
static void add_querydata_to_http_headers(gphadoop_context *context);
static size_t fill_buffer(gphadoop_context *context, char *start, size_t size);

/*
 * Clean up churl related data structures from the context.
 */
void
gpbridge_cleanup(gphadoop_context *context)
{
	if (context == NULL)
		return;

	churl_cleanup(context->churl_handle, false);
	context->churl_handle = NULL;

	churl_headers_cleanup(context->churl_headers);
	context->churl_headers = NULL;

	if (context->gphd_uri != NULL)
	{
		freeGPHDUri(context->gphd_uri);
		context->gphd_uri = NULL;
	}

	if (context->filterstr != NULL)
	{
		pfree(context->filterstr);
		context->filterstr = NULL;
	}
}

/*
 * Sets up data before starting import
 */
void
gpbridge_import_start(gphadoop_context *context)
{
	build_uri_for_read(context);
	context->churl_headers = churl_headers_init();
	add_querydata_to_http_headers(context);

	context->churl_handle = churl_init_download(context->uri.data, context->churl_headers);

	/* read some bytes to make sure the connection is established */
	churl_read_check_connectivity(context->churl_handle);
}

/*
 * Sets up data before starting export
 */
void
gpbridge_export_start(gphadoop_context *context)
{
	elog(DEBUG2, "pxf: file name for write: %s", context->gphd_uri->data);
	build_uri_for_write(context);
	context->churl_headers = churl_headers_init();
	add_querydata_to_http_headers(context);

	context->churl_handle = churl_init_upload(context->uri.data, context->churl_headers);
}

/*
 * Reads data from the PXF server into the given buffer of a given size
 */
int
gpbridge_read(gphadoop_context *context, char *databuf, int datalen)
{
	size_t		n = fill_buffer(context, databuf, datalen);

	if (n == 0)
	{
		context->completed = true;
		/* check if the connection terminated with an error */
		churl_read_check_connectivity(context->churl_handle);
	}

	elog(DEBUG5, "pxf gpbridge_read: segment %d read %zu bytes from %s",
		 GpIdentity.segindex, n, context->gphd_uri->data);

	return (int) n;
}

/*
 * Writes data from the given buffer of a given size to the PXF server
 */
int
gpbridge_write(gphadoop_context *context, char *databuf, int datalen)
{
	size_t		n = 0;

	if (datalen > 0)
	{
		n = churl_write(context->churl_handle, databuf, datalen);
		elog(DEBUG5, "pxf gpbridge_write: segment %d wrote %zu bytes to %s",
			GpIdentity.segindex, n, context->gphd_uri->data);
	}

	return (int) n;
}

/*
 * Format the URI for reading by adding PXF service endpoint details
 */
static void
build_uri_for_read(gphadoop_context *context)
{
	resetStringInfo(&context->uri);
	appendStringInfo(&context->uri, "http://%s/%s/read",
					 get_authority(), PXF_SERVICE_PREFIX);

	if ((LOG >= log_min_messages) || (LOG >= client_min_messages))
	{
		appendStringInfo(&context->uri, "?trace=true");
	}

	elog(DEBUG2, "pxf: uri %s for read", context->uri.data);
}

/*
 * Format the URI for writing by adding PXF service endpoint details
 */
static void
build_uri_for_write(gphadoop_context *context)
{
	appendStringInfo(&context->uri, "http://%s/%s/write",
					 get_authority(), PXF_SERVICE_PREFIX);

	if ((LOG >= log_min_messages) || (LOG >= client_min_messages))
	{
		appendStringInfo(&context->uri, "?trace=true");
	}

	elog(DEBUG2, "pxf: uri %s with file name for write: %s",
		context->uri.data, context->gphd_uri->data);
}


/*
 * Add key/value pairs to connection header. These values are the context of the query and used
 * by the remote component.
 */
static void
add_querydata_to_http_headers(gphadoop_context *context)
{
	PxfInputData inputData = {0};

	inputData.headers   = context->churl_headers;
	inputData.gphduri   = context->gphd_uri;
	inputData.rel       = context->relation;
	inputData.filterstr = context->filterstr;
	inputData.proj_info = context->proj_info;
	inputData.quals     = context->quals;
	build_http_headers(&inputData);
}

/*
 * Read data from churl until the buffer is full or there is no more data to be read
 */
static size_t
fill_buffer(gphadoop_context *context, char *start, size_t size)
{

	size_t		n;
	char	   *ptr = start;
	char	   *end = ptr + size;

	while (ptr < end)
	{
		n = churl_read(context->churl_handle, ptr, end - ptr);
		if (n == 0)
			break;

		ptr += n;
	}

	return ptr - start;
}
