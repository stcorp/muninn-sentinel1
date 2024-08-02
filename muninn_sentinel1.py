import os
import re
import json
import tarfile
import zipfile
from datetime import datetime
from xml.etree.ElementTree import parse

from muninn.schema import Mapping, Text, Integer, Timestamp
from muninn.geometry import Point, LinearRing, Polygon, MultiPoint, MultiPolygon
from muninn.struct import Struct
from muninn.util import copy_path


# Namespaces

class Sentinel1Namespace(Mapping):
    mission = Text(index=True, optional=True)  # S1, S1A, S1B, S1C, S1D
    mode = Text(index=True, optional=True)  # SM, EW, IW, WV, EN, AN, RFC
    mode_beam = Text(index=True, optional=True)  # S1, S2, ..., EW, IW, WV, ...
    product_type = Text(index=True, optional=True)  # RAW, SLC, GRD, OCN
    resolution_class = Text(index=True, optional=True)  # M, H, F
    processing_level = Integer(index=True, optional=True)  # 0, 1, 2
    product_class = Text(index=True, optional=True)  # S, A, C, N
    polarisation = Text(index=True, optional=True)  # SH, SV, DH, DV
    absolute_orbit = Integer(index=True, optional=True)
    relative_orbit = Integer(index=True, optional=True)
    orbit_direction = Text(index=True, optional=True)
    datatake_id = Integer(index=True, optional=True)
    instr_conf_id = Integer(index=True, optional=True)
    processing_facility = Text(index=True, optional=True)
    processor_name = Text(index=True, optional=True)
    processor_version = Text(index=True, optional=True)
    timeliness = Text(index=True, optional=True)
    cycle = Integer(index=True, optional=True)
    downlink_date = Timestamp(index=True, optional=True)


def namespaces():
    return ["sentinel1"]


def namespace(namespace_name):
    return Sentinel1Namespace


# Product types

L0_PRODUCT_TYPES = [
    'EN_RAW__0S',
    'EW_RAW__0A', 'EW_RAW__0C', 'EW_RAW__0N', 'EW_RAW__0S',
    'IW_RAW__0A', 'IW_RAW__0C', 'IW_RAW__0N', 'IW_RAW__0S',
    'N1_RAW__0S',
    'N2_RAW__0S',
    'N3_RAW__0S',
    'N4_RAW__0S',
    'N5_RAW__0S',
    'N6_RAW__0S',
    'S1_RAW__0A', 'S1_RAW__0C', 'S1_RAW__0N', 'S1_RAW__0S',
    'S2_RAW__0A', 'S2_RAW__0C', 'S2_RAW__0N', 'S2_RAW__0S',
    'S3_RAW__0A', 'S3_RAW__0C', 'S3_RAW__0N', 'S3_RAW__0S',
    'S4_RAW__0A', 'S4_RAW__0C', 'S4_RAW__0N', 'S4_RAW__0S',
    'S5_RAW__0A', 'S5_RAW__0C', 'S5_RAW__0N', 'S5_RAW__0S',
    'S6_RAW__0A', 'S6_RAW__0C', 'S6_RAW__0N', 'S6_RAW__0S',
    'WV_RAW__0A', 'WV_RAW__0C', 'WV_RAW__0N', 'WV_RAW__0S',
    'RF_RAW__0S',
    'Z1_RAW__0S',
    'Z2_RAW__0S',
    'Z3_RAW__0S',
    'Z4_RAW__0S',
    'Z5_RAW__0S',
    'Z6_RAW__0S',
    'ZE_RAW__0S',
    'ZI_RAW__0S',
    'ZW_RAW__0S',
    ]

L1_PRODUCT_TYPES = [
    'EN_GRDF_1S',
    'EN_GRDH_1S',
    'EN_GRDM_1S',
    'EN_SLC__1S',
    'EW_GRDH_1A', 'EW_GRDH_1S',
    'EW_GRDM_1A', 'EW_GRDM_1S',
    'EW_SLC__1A', 'EW_SLC__1S',
    'IW_GRDH_1A', 'IW_GRDH_1S',
    'IW_GRDM_1A', 'IW_GRDM_1S',
    'IW_SLC__1A', 'IW_SLC__1S',
    'N1_GRDF_1A', 'N1_GRDF_1S',
    'N1_GRDH_1A', 'N1_GRDH_1S',
    'N1_GRDM_1A', 'N1_GRDM_1S',
    'N1_SLC__1A', 'N1_SLC__1S',
    'N2_GRDF_1A', 'N2_GRDF_1S',
    'N2_GRDH_1A', 'N2_GRDH_1S',
    'N2_GRDM_1A', 'N2_GRDM_1S',
    'N2_SLC__1A', 'N2_SLC__1S',
    'N3_GRDF_1A', 'N3_GRDF_1S',
    'N3_GRDH_1A', 'N3_GRDH_1S',
    'N3_GRDM_1A', 'N3_GRDM_1S',
    'N3_SLC__1A', 'N3_SLC__1S',
    'N4_GRDF_1A', 'N4_GRDF_1S',
    'N4_GRDH_1A', 'N4_GRDH_1S',
    'N4_GRDM_1A', 'N4_GRDM_1S',
    'N4_SLC__1A', 'N4_SLC__1S',
    'N5_GRDF_1A', 'N5_GRDF_1S',
    'N5_GRDH_1A', 'N5_GRDH_1S',
    'N5_GRDM_1A', 'N5_GRDM_1S',
    'N5_SLC__1A', 'N5_SLC__1S',
    'N6_GRDF_1A', 'N6_GRDF_1S',
    'N6_GRDH_1A', 'N6_GRDH_1S',
    'N6_GRDM_1A', 'N6_GRDM_1S',
    'N6_SLC__1A', 'N6_SLC__1S',
    'S1_GRDF_1A', 'S1_GRDF_1S',
    'S1_GRDH_1A', 'S1_GRDH_1S',
    'S1_GRDM_1A', 'S1_GRDM_1S',
    'S1_SLC__1A', 'S1_SLC__1S',
    'S2_GRDF_1A', 'S2_GRDF_1S',
    'S2_GRDH_1A', 'S2_GRDH_1S',
    'S2_GRDM_1A', 'S2_GRDM_1S',
    'S2_SLC__1A', 'S2_SLC__1S',
    'S3_GRDF_1A', 'S3_GRDF_1S',
    'S3_GRDH_1A', 'S3_GRDH_1S',
    'S3_GRDM_1A', 'S3_GRDM_1S',
    'S3_SLC__1A', 'S3_SLC__1S',
    'S4_GRDF_1A', 'S4_GRDF_1S',
    'S4_GRDH_1A', 'S4_GRDH_1S',
    'S4_GRDM_1A', 'S4_GRDM_1S',
    'S4_SLC__1A', 'S4_SLC__1S',
    'S5_GRDF_1A', 'S5_GRDF_1S',
    'S5_GRDH_1A', 'S5_GRDH_1S',
    'S5_GRDM_1A', 'S5_GRDM_1S',
    'S5_SLC__1A', 'S5_SLC__1S',
    'S6_GRDF_1A', 'S6_GRDF_1S',
    'S6_GRDH_1A', 'S6_GRDH_1S',
    'S6_GRDM_1A', 'S6_GRDM_1S',
    'S6_SLC__1A', 'S6_SLC__1S',
    'WV_SLC__1A', 'WV_SLC__1S',
    ]

L2_PRODUCT_TYPES = [
    'EW_OCN__2A', 'EW_OCN__2S',
    'IW_OCN__2A', 'IW_OCN__2S',
    'S1_OCN__2A', 'S1_OCN__2S',
    'S2_OCN__2A', 'S2_OCN__2S',
    'S3_OCN__2A', 'S3_OCN__2S',
    'S4_OCN__2A', 'S4_OCN__2S',
    'S5_OCN__2A', 'S5_OCN__2S',
    'S6_OCN__2A', 'S6_OCN__2S',
    'WV_OCN__2A', 'WV_OCN__2S',
    ]

ETAD_PRODUCT_TYPES = [
    'EW_ETA__AX',
    'IW_ETA__AX',
    'S1_ETA__AX',
    'S2_ETA__AX',
    'S3_ETA__AX',
    'S4_ETA__AX',
    'S5_ETA__AX',
    'S6_ETA__AX',
    ]

RVL_PRODUCT_TYPES = [
    'IW_RVC__2S',
]

AUX_SAFE_PRODUCT_TYPES = [
    'AUX_CAL',
    'AUX_INS',
    'AUX_PP1',
    'AUX_PP2',
    'AUX_SCS',
    'AUX_ML2',
    'AUX_ICE',
    'AUX_WAV',
    'AUX_WND',
    'AUX_ITC',
    'AUX_SCF',
    'AUX_TEC',
    'AUX_TRO',
    ]

AUX_EOF_PRODUCT_TYPES = [
    'AMH_ERRMAT',
    'AMV_ERRMAT',
    'AUX_POEORB',
    'AUX_PREORB',
    'AUX_RESATT',
    'AUX_RESORB',
    ]

MUNINN_PRODUCT_TYPES = L0_PRODUCT_TYPES + L1_PRODUCT_TYPES + L2_PRODUCT_TYPES + AUX_SAFE_PRODUCT_TYPES + \
    AUX_EOF_PRODUCT_TYPES


def parse_datetime(str):
    if str.endswith('Z'):
        str = str[:-1]
    try:
        return datetime.strptime(str, "%Y-%m-%dT%H:%M:%S.%f")
    except Exception:
        return datetime.strptime(str, "%Y-%m-%dT%H:%M:%S")


def package_zip(paths, target_filepath):
    with zipfile.ZipFile(target_filepath, "x", zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
        for path in paths:
            rootlen = len(os.path.dirname(path)) + 1
            for base, dirs, files in os.walk(path):
                for file in files:
                    fn = os.path.join(base, file)
                    archive.write(fn, fn[rootlen:])


class Sentinel1Product(object):

    def __init__(self, product_type):
        self.product_type = product_type
        self.filename_pattern = None

    @property
    def hash_type(self):
        return "md5"

    @property
    def namespaces(self):
        return ["sentinel1"]

    @property
    def use_enclosing_directory(self):
        return False

    def parse_filename(self, filename):
        match = re.match(self.filename_pattern, os.path.basename(filename))
        if match:
            return match.groupdict()
        return None

    def identify(self, paths):
        if len(paths) != 1:
            return False
        return re.match(self.filename_pattern, os.path.basename(paths[0])) is not None

    def archive_path(self, properties):
        name_attrs = self.parse_filename(properties.core.physical_name)
        mission = name_attrs['mission']
        if mission[2] == "_":
            mission = mission[0:2]
        return os.path.join(
            mission,
            self.product_type,
            name_attrs['validity_start'][0:4],
            name_attrs['validity_start'][4:6],
            name_attrs['validity_start'][6:8],
        )


class SAFEProduct(Sentinel1Product):

    def __init__(self, product_type, zipped=False):
        self.product_type = product_type
        self.zipped = zipped
        pattern = [
            r"^(?P<mission>S1(_|A|B|C|D))",
            r"(?P<product_type>%s)(?P<polarisation>.{2})" % product_type,
            r"(?P<validity_start>[\dT]{15})",
            r"(?P<validity_stop>[\dT]{15})",
            r"(?P<absolute_orbit>[\d]{6})",
            r"(?P<datatake_id>.{6})",
            r"(?P<crc>.{4})",
        ]
        if zipped:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE\.zip$"
        else:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE$"

    def _get_footprint_from_manifest(self, root):
        ns = {"safe": "http://www.esa.int/safe/sentinel-1.0",
              "gml": "http://www.opengis.net/gml"}
        coordinates_set = [x.text for x in root.findall(".//safe:frame/safe:footPrint/gml:coordinates", ns)]
        if len(coordinates_set) == 1 and len(' '.join(coordinates_set[0].split(',')).split()) <= 4:
            # we only have two points -> use a multipoint
            points = MultiPoint()
            coord = ' '.join(coordinates_set[0].split(',')).split()
            for lat, lon in zip(coord[0::2], coord[1::2]):
                points.append(Point(float(lon), float(lat)))
            return points
        polygons = MultiPolygon()
        for coordinates in coordinates_set:
            coord = ' '.join(coordinates.split(',')).split()
            linearring = LinearRing([Point(float(lon), float(lat)) for lat, lon in
                                     zip(coord[0::2], coord[1::2])])
            polygons.append(Polygon([linearring]))
        return polygons

    def _analyze_manifest(self, root, properties):
        ns = {"safe": "http://www.esa.int/safe/sentinel-1.0",
              "s1": "http://www.esa.int/safe/sentinel-1.0/sentinel-1",
              "s1sar": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar"}
        if "processing_level" in properties.sentinel1:
            if properties.sentinel1.processing_level == 1 or self.product_type[8:10] == "2A":
                ns["s1sar"] = "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-1"
            elif properties.sentinel1.processing_level == 2:
                ns["s1sar"] = "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-2"
        elif properties.sentinel1.product_type == "ETA":
            ns["s1sar"] = "http://www.esa.int/safe/sentinel-1.0/sentinel-1/sar/level-1"
        acquisition_period = root.find(".//safe:acquisitionPeriod", ns)
        core = properties.core
        core.validity_start = parse_datetime(acquisition_period.find("./safe:startTime", ns).text)
        core.validity_stop = parse_datetime(acquisition_period.find("./safe:stopTime", ns).text)
        processing = root.find(".//xmlData/safe:processing", ns)
        core.creation_date = parse_datetime(processing.get("stop"))
        core.footprint = self._get_footprint_from_manifest(root)

        sentinel1 = properties.sentinel1
        orbit_reference = root.find(".//safe:orbitReference", ns)
        sentinel1.absolute_orbit = int(orbit_reference.find("./safe:orbitNumber[@type='start']", ns).text)
        sentinel1.relative_orbit = int(orbit_reference.find("./safe:relativeOrbitNumber[@type='start']", ns).text)
        sentinel1.cycle = int(orbit_reference.find("./safe:cycleNumber", ns).text)
        orbit_pass = orbit_reference.find("./safe:extension/s1:orbitProperties/s1:pass", ns)
        if orbit_pass is not None:
            sentinel1.orbit_direction = orbit_pass.text.lower()
        sentinel1.instr_conf_id = int(root.find(".//s1sar:instrumentConfigurationID", ns).text)
        downlinks = root.findall(".//safe:resource[@name='Downlinked Stream'][@role='Raw Data']/safe:processing", ns)
        if downlinks:
            sentinel1.downlink_date = max([parse_datetime(x.get("stop")) for x in downlinks])
        facility = processing.find("./safe:facility", ns)
        if facility is not None:
            sentinel1.processing_facility = facility.get("site")
        software = processing.find("./safe:software", ns)
        if software is None:
            software = processing.find("./safe:facility/safe:software", ns)
        if software is not None:
            sentinel1.processor_name = software.get("name")
            sentinel1.processor_version = software.get("version")
        timeliness = root.find(".//s1sar:productTimelinessCategory", ns)
        if timeliness is not None:
            sentinel1.timeliness = timeliness.text

    def read_xml_component(self, filepath, componentpath):
        if self.zipped:
            componentpath = os.path.join(os.path.splitext(os.path.basename(filepath))[0], componentpath)
            with zipfile.ZipFile(filepath) as zproduct:
                with zproduct.open(componentpath) as manifest:
                    return parse(manifest).getroot()
        else:
            with open(os.path.join(filepath, componentpath)) as manifest:
                return parse(manifest).getroot()

    def analyze(self, paths, filename_only=False):
        inpath = paths[0]
        name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        if self.zipped:
            core.product_name = os.path.splitext(core.product_name)[0]
        core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")
        core.validity_stop = datetime.strptime(name_attrs['validity_stop'], "%Y%m%dT%H%M%S")

        sentinel1 = properties.sentinel1 = Struct()
        sentinel1.mission = name_attrs['mission']
        if sentinel1.mission[2] == "_":
            sentinel1.mission = sentinel1.mission[0:2]
        sentinel1.mode_beam = self.product_type[0:2]
        sentinel1.mode = sentinel1.mode_beam
        if sentinel1.mode[0] == 'S':
            sentinel1.mode = "SM"
        elif sentinel1.mode[0] == 'N':
            sentinel1.mode = "AN"
        sentinel1.product_type = self.product_type[3:6]
        if self.product_type[6:7] != '_':
            sentinel1.resolution_class = self.product_type[6:7]
        try:
            sentinel1.processing_level = int(self.product_type[8:9])
        except Exception:
            pass
        sentinel1.product_class = self.product_type[9:10]
        sentinel1.polarisation = name_attrs['polarisation']
        sentinel1.absolute_orbit = int(name_attrs['absolute_orbit'])
        sentinel1.datatake_id = int(name_attrs['datatake_id'], 16)

        if not filename_only:
            # Update properties based on manifest content
            self._analyze_manifest(self.read_xml_component(inpath, "manifest.safe"), properties)

        return properties

    def export_zip(self, archive, properties, target_path, paths):
        if self.zipped:
            assert len(paths) == 1, "zipped product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name + ".zip")
        package_zip(paths, target_filepath)
        return target_filepath


class AUXProduct(SAFEProduct):

    def __init__(self, product_type, zipped=False):
        self.product_type = product_type
        self.zipped = zipped
        pattern = [
            r"^(?P<mission>S1(_|A|B|C|D))",
            r"(?P<product_type>%s)" % product_type,
            r"V(?P<validity_start>[\dT]{15})",
            r"G(?P<generation_date>[\dT]{15})",
        ]
        if zipped:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE\.zip$"
        else:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE$"

    def _analyze_manifest(self, root, properties):
        ns = {"safe": "http://www.esa.int/safe/sentinel-1.0",
              "s1auxsar": "http://www.esa.int/safe/sentinel-1.0/sentinel-1/auxiliary/sar"}
        properties.core.validity_start = parse_datetime(root.find(".//s1auxsar:validity", ns).text)
        properties.core.validity_stop = datetime.max
        properties.core.creation_date = parse_datetime(root.find(".//s1auxsar:generation", ns).text)
        properties.sentinel1.instr_conf_id = int(root.find(".//s1auxsar:instrumentConfigurationId", ns).text)
        properties.sentinel1.processing_facility = root.find(".//safe:facility", ns).get("site")

    def analyze(self, paths, filename_only=False):
        inpath = paths[0]
        name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        if self.zipped:
            core.product_name = os.path.splitext(core.product_name)[0]
        core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")
        core.validity_stop = datetime.max
        core.creation_date = datetime.strptime(name_attrs['generation_date'], "%Y%m%dT%H%M%S")

        sentinel1 = properties.sentinel1 = Struct()
        sentinel1.mission = name_attrs['mission']
        if sentinel1.mission[2] == "_":
            sentinel1.mission = sentinel1.mission[0:2]

        if not filename_only:
            # Update properties based on manifest content
            self._analyze_manifest(self.read_xml_component(inpath, "manifest.safe"), properties)

        return properties

    def export_zip(self, archive, properties, target_path, paths):
        if self.zipped:
            assert len(paths) == 1, "zipped product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name + ".zip")
        package_zip(paths, target_filepath)
        return target_filepath


class EOFProduct(Sentinel1Product):

    def __init__(self, product_type, split=False, zipped=False):
        self.product_type = product_type
        self.split = split
        self.zipped = zipped
        self.xml_namespace = {}
        pattern = [
            r"(?P<mission>S1(_|A|B|C|D))",
            r"(?P<file_class>.{4})",
            r"(?P<product_type>%s)" % product_type,
            r"(?P<processing_facility>.{4})",
            r"(?P<creation_date>[\dT]{15})",
            r"V(?P<validity_start>[\dT]{15})",
            r"(?P<validity_stop>[\dT]{15})"
        ]
        self.filename_pattern = "_".join(pattern)
        if self.split:
            if self.zipped:
                self.filename_pattern += r"\.TGZ$"
        else:
            if self.zipped:
                self.filename_pattern += r"\.EOF\.zip$"
            else:
                self.filename_pattern += r"\.EOF$"

    @property
    def use_enclosing_directory(self):
        return self.split and not self.zipped

    def enclosing_directory(self, properties):
        return properties.core.product_name

    def identify(self, paths):
        if self.split and not self.zipped:
            if len(paths) != 2:
                return False
            paths = sorted(paths)
            if re.match(self.filename_pattern + r"\.DBL$", os.path.basename(paths[0])) is None:
                return False
            if re.match(self.filename_pattern + r"\.HDR$", os.path.basename(paths[1])) is None:
                return False
            return True
        else:
            if len(paths) != 1:
                return False
            return re.match(self.filename_pattern, os.path.basename(paths[0])) is not None

    def read_xml_header(self, filepath):
        if self.split:
            if self.zipped:
                hdrpath = os.path.splitext(os.path.basename(filepath))[0] + ".HDR"
                with tarfile.open(filepath, "r:gz") as tar:
                    return parse(tar.extractfile(hdrpath)).getroot()
            else:
                with open(filepath) as hdrfile:
                    return parse(hdrfile).getroot()
        else:
            ns = self.xml_namespace
            if self.zipped:
                with zipfile.ZipFile(filepath) as zproduct:
                    eofpath = os.path.splitext(os.path.basename(filepath))[0] + ".EOF"
                    with zproduct.open(eofpath) as eoffile:
                        return parse(eoffile).getroot().find("./Earth_Explorer_Header", ns)
            else:
                with open(filepath) as eoffile:
                    return parse(eoffile).getroot().find("./Earth_Explorer_Header", ns)

    def analyze(self, paths, filename_only=False):
        if self.split and not self.zipped:
            name_attrs = self.parse_filename(os.path.splitext(os.path.basename(paths[0]))[0])
            inpath = sorted(paths)[-1]  # use the .HDR for metadata extraction
        else:
            inpath = paths[0]
            name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        if 'creation_date' in name_attrs:
            core.creation_date = datetime.strptime(name_attrs['creation_date'], "%Y%m%dT%H%M%S")
        core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")
        if name_attrs['validity_stop'] == "99999999T999999":
            core.validity_stop = datetime.max
        else:
            core.validity_stop = datetime.strptime(name_attrs['validity_stop'], "%Y%m%dT%H%M%S")

        sentinel1 = properties.sentinel1 = Struct()
        sentinel1.mission = name_attrs['mission']
        if sentinel1.mission[2] == "_":
            sentinel1.mission = sentinel1.mission[0:2]
        if 'processing_facility' in name_attrs:
            sentinel1.processing_facility = name_attrs['processing_facility']

        if not filename_only:
            header = self.read_xml_header(inpath)
            ns = self.xml_namespace
            validity_start = header.find("./Fixed_Header/Validity_Period/Validity_Start", ns).text
            core.validity_start = datetime.strptime(validity_start, "UTC=%Y-%m-%dT%H:%M:%S")
            validity_stop = header.find("./Fixed_Header/Validity_Period/Validity_Stop", ns).text
            if validity_stop == "UTC=9999-99-99T99:99:99":
                core.validity_stop = datetime.max
            else:
                core.validity_stop = datetime.strptime(validity_stop, "UTC=%Y-%m-%dT%H:%M:%S")
            creation_date = header.find("./Fixed_Header/Source/Creation_Date", ns).text
            core.creation_date = datetime.strptime(creation_date, "UTC=%Y-%m-%dT%H:%M:%S")
            sentinel1.processing_facility = header.find("./Fixed_Header/Source/System", ns).text
            sentinel1.processor_name = header.find("./Fixed_Header/Source/Creator", ns).text
            sentinel1.processor_version = header.find("./Fixed_Header/Source/Creator_Version", ns).text
            configuration_identifier = header.find("./Variable_Header/Configuration_Identifier", ns)
            if configuration_identifier is not None:
                sentinel1.instr_conf_id = int(configuration_identifier.text)

        return properties


class RVLProduct(Sentinel1Product):

    def __init__(self, product_type):
        self.product_type = product_type
        pattern = [
            r"(?P<mission>S1(_|A|B|C|D))",
            r"(?P<product_type>%s)(?P<polarisation>.{2})" % product_type,
            r"(?P<validity_start>[\dT]{15})",
            r"(?P<validity_stop>[\dT]{15})",
            r"(?P<absolute_orbit>[\d]{6})",
            r"(?P<datatake_id>.{6})",
            r"(?P<crc>.{6})",
        ]
        self.filename_pattern = "_".join(pattern) + r"\.nc$"

    def _analyze_netcdf(self, filepath, properties):
        try:
            import coda
        except ImportError:
            return
        with coda.open(filepath) as pf:
            properties.core.validity_start = datetime.strptime(coda.fetch(pf, "@time_coverage_start"),
                                                               "%Y-%m-%dT%H:%M:%S.%fZ")
            properties.core.validity_stop = datetime.strptime(coda.fetch(pf, "@time_coverage_end"),
                                                              "%Y-%m-%dT%H:%M:%S.%fZ")
            properties.core.creation_date = datetime.strptime(coda.fetch(pf, "@date_created"),
                                                              "%Y-%m-%dT%H:%M:%S.%fZ")
            footprint = json.loads(coda.fetch(pf, "@footprint"))
            assert footprint["type"] == "FeatureCollection"
            polygons = MultiPolygon()
            for feature in footprint["features"]:
                coordinates = feature["geometry"]["coordinates"][0]
                polygons.append(Polygon([LinearRing([Point(float(c[0]), float(c[1])) for c in coordinates])]))
            if len(polygons) == 1:
                properties.core.footprint = polygons[0]
            else:
                properties.core.footprint = polygons
            properties.sentinel1.relative_orbit = int(coda.fetch(pf, "@relative_orbit")[0])
            properties.sentinel1.orbit_direction = coda.fetch(pf, "@orbit_direction")
            properties.sentinel1.processing_facility = coda.fetch(pf, "@processing_center")
            properties.sentinel1.processor_name = coda.fetch(pf, "@processor_name")
            properties.sentinel1.processor_version = coda.fetch(pf, "@processor_version")
            properties.sentinel1.cycle = int(coda.fetch(pf, "@cycle")[0])

    def analyze(self, paths, filename_only=False):
        inpath = paths[0]
        name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")
        core.validity_stop = datetime.strptime(name_attrs['validity_stop'], "%Y%m%dT%H%M%S")

        sentinel1 = properties.sentinel1 = Struct()
        sentinel1.mission = name_attrs['mission']
        if sentinel1.mission[2] == "_":
            sentinel1.mission = sentinel1.mission[0:2]
        sentinel1.mode_beam = self.product_type[0:2]
        sentinel1.mode = sentinel1.mode_beam
        sentinel1.product_type = self.product_type[3:6]
        sentinel1.processing_level = int(self.product_type[8:9])
        sentinel1.product_class = self.product_type[9:10]
        sentinel1.polarisation = name_attrs['polarisation']
        sentinel1.absolute_orbit = int(name_attrs['absolute_orbit'])
        sentinel1.datatake_id = int(name_attrs['datatake_id'], 16)

        if not filename_only:
            self._analyze_netcdf(inpath, properties)

        return properties


_product_types = dict(
    [(product_type, SAFEProduct(product_type)) for product_type in L0_PRODUCT_TYPES] +
    [(product_type, SAFEProduct(product_type)) for product_type in L1_PRODUCT_TYPES] +
    [(product_type, SAFEProduct(product_type)) for product_type in L2_PRODUCT_TYPES] +
    [(product_type, SAFEProduct(product_type)) for product_type in ETAD_PRODUCT_TYPES] +
    [(product_type, RVLProduct(product_type)) for product_type in RVL_PRODUCT_TYPES] +
    [(product_type, AUXProduct(product_type)) for product_type in AUX_SAFE_PRODUCT_TYPES] +
    [(product_type, EOFProduct(product_type)) for product_type in AUX_EOF_PRODUCT_TYPES]
)


def product_types():
    return _product_types.keys()


def product_type_plugin(product_type):
    return _product_types.get(product_type)
