import logging
import mimetypes
import os
import requests

from contextlib import closing
from django.core.exceptions import ValidationError
from django.core.files.temp import NamedTemporaryFile
from django.core.files.uploadedfile import InMemoryUploadedFile
from django.core.validators import URLValidator
from django.db import models
from django.conf import settings
from hashlib import md5
from onadata.apps.logger.models import XForm
from onadata.apps.main.forms import MapboxLayerForm

CHUNK_SIZE = 1024

urlvalidate = URLValidator()


def is_valid_url(uri):
    try:
        urlvalidate(uri)
    except ValidationError:
        return False

    return True


def upload_to(instance, filename):
    if instance.data_type == 'media':
        return os.path.join(
            instance.xform.user.username,
            'formid-media',
            filename
        )
    return os.path.join(
        instance.xform.user.username,
        'docs',
        filename
    )


def unique_type_for_form(xform, data_type, data_value=None, data_file=None,
                         form_metadata=None):
    all_matches = type_for_form(xform, data_type, form_metadata)
    modified = False
    if not len(all_matches):
        result = MetaData(data_type=data_type, xform=xform)
        modified = True
    else:
        result = all_matches[0]
    if data_value:
        result.data_value = data_value
        modified = True
    if data_file:
        if result.data_value is None or result.data_value == '':
            result.data_value = data_file.name
        result.data_file = data_file
        result.data_file_type = data_file.content_type
        modified = True
    if modified:
        result.save()
    return result


def type_for_form(xform, data_type, form_metadata=None):
    # If form_metadata is specified, the database is not queried; instead, the
    # ordered collection of MetaData objects - retrieved from metadata_for_form
    # - is filtered for MetaData objects with the appropriate data_type.
    if form_metadata:
        return [m for m in form_metadata if m.data_type == data_type]
    return MetaData.objects.filter(xform=xform, data_type=data_type)\
        .order_by(-id)


def metadata_for_form(xform):
    # Order all MetaData objects to be consistent with external_export
    # expectations and to have a guaranteed iteration order.
    return MetaData.objects.filter(xform=xform).order_by('-id')


def create_media(media):
    """Download media link"""
    if is_valid_url(media.data_value):
        filename = media.data_value.split('/')[-1]
        data_file = NamedTemporaryFile()
        content_type = mimetypes.guess_type(filename)
        with closing(requests.get(media.data_value, stream=True)) as r:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    data_file.write(chunk)
        data_file.seek(os.SEEK_SET, os.SEEK_END)
        size = os.path.getsize(data_file.name)
        data_file.seek(os.SEEK_SET)
        media.data_value = filename
        media.data_file = InMemoryUploadedFile(
            data_file, 'data_file', filename, content_type,
            size, charset=None)

        return media

    return None


def media_resources(media_list, download=False):
    """List of MetaData objects of type media

    @param media_list - list of MetaData objects of type `media`
    @param download - boolean, when True downloads media files when
                      media.data_value is a valid url

    return a list of MetaData objects
    """
    data = []
    for media in media_list:
        if media.data_file.name == '' and download:
            media = create_media(media)

            if media:
                data.append(media)
        else:
            data.append(media)

    return data


class MetaData(models.Model):
    xform = models.ForeignKey(XForm)
    data_type = models.CharField(max_length=255)
    data_value = models.CharField(max_length=255)
    data_file = models.FileField(upload_to=upload_to, blank=True, null=True)
    data_file_type = models.CharField(max_length=255, blank=True, null=True)
    file_hash = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        app_label = 'main'
        unique_together = ('xform', 'data_type', 'data_value')

    def save(self, *args, **kwargs):
        self._set_hash()
        super(MetaData, self).save(*args, **kwargs)

    @property
    def hash(self):
        if self.file_hash is not None and self.file_hash != '':
            return self.file_hash
        else:
            return self._set_hash()

    def _set_hash(self):
        if not self.data_file:
            return None

        file_exists = self.data_file.storage.exists(self.data_file.name)

        if (file_exists and self.data_file.name != '') \
                or (not file_exists and self.data_file):
            try:
                self.data_file.seek(os.SEEK_SET)
            except IOError:
                return u''
            else:
                self.file_hash = u'md5:%s' \
                    % md5(self.data_file.read()).hexdigest()

                return self.file_hash

        return u''

    @staticmethod
    def public_link(xform, data_value=None, form_metadata=None):
        data_type = 'public_link'
        if data_value is False:
            data_value = 'False'
        metadata = unique_type_for_form(xform, data_type, data_value,
                                        form_metadata)
        # make text field a boolean
        if metadata.data_value == 'True':
            return True
        else:
            return False

    @staticmethod
    def form_license(xform, data_value=None, form_metadata=None):
        data_type = 'form_license'
        return unique_type_for_form(xform, data_type, data_value, form_metadata)

    @staticmethod
    def data_license(xform, data_value=None, form_metadata=None):
        data_type = 'data_license'
        return unique_type_for_form(xform, data_type, data_value, form_metadata)

    @staticmethod
    def source(xform, data_value=None, data_file=None, form_metadata=None):
        data_type = 'source'
        return unique_type_for_form(xform, data_type, data_value, data_file,
                                    form_metadata)

    @staticmethod
    def supporting_docs(xform, data_file=None, form_metadata=None):
        data_type = 'supporting_doc'
        if data_file:
            doc = MetaData(data_type=data_type, xform=xform,
                           data_value=data_file.name,
                           data_file=data_file,
                           data_file_type=data_file.content_type)
            doc.save()
        return type_for_form(xform, data_type, form_metadata)

    @staticmethod
    def media_upload(xform, data_file=None, download=False, form_metadata=None):
        data_type = 'media'
        if data_file:
            allowed_types = settings.SUPPORTED_MEDIA_UPLOAD_TYPES
            content_type = data_file.content_type \
                if data_file.content_type in allowed_types else \
                mimetypes.guess_type(data_file.name)[0]
            if content_type in allowed_types:
                media = MetaData(data_type=data_type, xform=xform,
                                 data_value=data_file.name,
                                 data_file=data_file,
                                 data_file_type=content_type)
                media.save()
        return media_resources(type_for_form(xform, data_type, form_metadata),
                               download)

    @staticmethod
    def media_add_uri(xform, uri):
        """Add a uri as a media resource"""
        data_type = 'media'

        if is_valid_url(uri):
            media = MetaData(data_type=data_type, xform=xform,
                             data_value=uri)
            media.save()

    @staticmethod
    def mapbox_layer_upload(xform, data=None, form_metadata=None,
                            form_metadata=None):
        data_type = 'mapbox_layer'

        # Use a serialization/deserialization order independent of field
        # declaration order.
        keys = MapboxLayerForm.SERIALIZATION_ORDER

        data_string = None
        if data:
            data_string = '||'.join([data.get(key, '') for key in keys])
        mapbox_layer = unique_type_for_form(xform, data_type, data_string,
                                            form_metadata)
        if mapbox_layer.data_value:
            values = mapbox_layer.data_value.split('||')
            # If we can't deserialize the data_value object, log an error and
            # return nothing. The value can still be set, but this check will
            # prevent breaking the view because of corrupted stored data.
            if len(values) < len(keys):
                logging.error(
                    'Not enough fields to deserialize mapbox_layer object: %s'
                    % mapbox_layer)
                return None
            data_values = {k: values[i] for i, k in enumerate(keys)}
            data_values['id'] = mapbox_layer.id
            return data_values
        else:
            return None

    @staticmethod
    def external_export(xform, data_value=None):
        data_type = 'external_export'

        if data_value:
            result = MetaData(data_type=data_type, xform=xform,
                              data_value=data_value)
            result.save()
            return result

        return type_for_form(xform, data_type, form_metadata)

    @property
    def external_export_url(self):
        parts = self.data_value.split('|')

        return parts[1] if len(parts) > 1 else None

    @property
    def external_export_name(self):
        parts = self.data_value.split('|')

        return parts[0] if len(parts) > 1 else None

    @property
    def external_export_template(self):
        parts = self.data_value.split('|')

        return parts[1].replace('xls', 'templates') if len(parts) > 1 else None
