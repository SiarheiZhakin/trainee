import json
import xml.etree.ElementTree as ET


class LoaderJson:
    """load Json format on local storage"""
    def export(self, data, file:str)->None:
        with open(file, 'w',  encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            print(f'file saving on {file}')


class LoaderXml:
    """load Xml format on local storage"""
    def export(self, data, file:str)->None:
        root = ET.Element('queries')
        for k, v in data.items():
            query = ET.SubElement(root, 'query')
            query.text = k
            for i in v:
                ET.SubElement(query, 'Room').text = str(i)
        tree = ET.ElementTree(root)
        ET.indent(tree, space="\t", level=1)
        tree.write(file, encoding="utf-8", xml_declaration=True)
        print(f'file saving on {file}')

class ExportFile:
    """Use choice from cmd-line Xml or Json"""
    @staticmethod
    def export_file(format: str):
        if format == 'json':
            return LoaderJson()
        elif format == 'xml':
            return LoaderXml()
        else:
            raise ValueError(f'{format} format is not available')

