# formatters.py
import json
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod


class Formatter(ABC):
    """Abstract base class (interface) for formatters."""

    @abstractmethod
    def format(self, data: dict) -> str:
        pass


class JsonFormatter(Formatter):
    """Concrete strategy for JSON output."""

    def format(self, data: dict) -> str:
        return json.dumps(data, indent=4, default=str)  # default=str for handling dates/ages


class XmlFormatter(Formatter):
    """Concrete strategy for XML output."""

    def _dict_to_xml(self, parent_element, data):
        """Recursive helper to convert dict to XML."""
        if isinstance(data, dict):
            for key, value in data.items():
                element = ET.Element(parent_element, key)
                self._dict_to_xml(element, value)
        elif isinstance(data, list):
            for item in data:
                # For lists, we typically create an 'item' element or use the parent's name
                item_element_name = parent_element.tag
                if item_element_name.endswith('s'):  # remove 's' (e.g. rooms -> room)
                    item_element_name = item_element_name[:-1]
                else:
                    item_element_name = 'item'

                element = ET.Element(parent_element, item_element_name)
                self._dict_to_xml(element, item)
        else:
            parent_element.text = str(data)

    def format(self, data: dict) -> str:
        root = ET.Element("results")
        self._dict_to_xml(root, data)
        # tostring returns bytes, decode to str
        return ET.tostring(root, encoding='unicode', short_empty_elements=False)