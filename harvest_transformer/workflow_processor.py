import collections
import logging
import uuid

import yaml

from .utils import get_file_from_url

ORDERED = True

REQUIRED_COLLECTIONS_FIELDS = [
    "type",
    "stac_version",
    "stac_extensions",
    "id",
    "title",
    "description",
    "keywords",
    "license",
    "providers",
    "extent",
    "summaries",
    "links",
    "assets",
]

REQUIRED_COLLECTIONS_FIELDS_EXTENT = ["spatial", "temporal"]

REQUIRED_WORKFLOW_SUMMARIES_FIELDS = [
    "inputs",
    "outputs",
    "examples",
    "pricing",
    "documentation",
    "contact_information",
]

DEFAULT_EXTENT_FIELD = {
    "spatial": {"bbox": [[-180, -90, 180, 90]]},
    "temporal": {"interval": [[None, None]]},
}

DEFAULT_STAC_VERSION = "1.0.0"


class WorkflowProcessor:

    # Identify which STAC fields are missing that are required for a collection
    def workflow_check_missing_fields(self, file_json: dict) -> list:
        missing_fields = []
        for field in REQUIRED_COLLECTIONS_FIELDS:
            if field not in file_json or not file_json[field]:
                missing_fields.append(field)
            elif field == "extent":
                if "spatial" not in file_json[field]:
                    missing_fields.append("spatial")
                elif "bbox" not in file_json[field]["spatial"]:
                    missing_fields.append("bbox")

                if "temporal" not in file_json[field]:
                    missing_fields.append("temporal")
                elif "interval" not in file_json[field]["temporal"]:
                    missing_fields.append("interval")
                elif not file_json[field]["temporal"]["interval"]:
                    missing_fields.append("interval")

            elif field == "summaries":
                if "inputs" not in file_json["summaries"] or not file_json["summaries"]["inputs"]:
                    missing_fields.append("inputs")
                if "outputs" not in file_json["summaries"] or not file_json["summaries"]["outputs"]:
                    missing_fields.append("outputs")

        return missing_fields

    def update_file(self, file_name: str, source: str, file_json: dict, **kwargs) -> dict:

        if ("assets" not in file_json) or (
            "assets" in file_json and "cwl_script" not in file_json["assets"]
        ):
            return file_json

        stac_collection_raw = file_json

        scrape_cwl = True

        # Confirm required workflow assets are provided (>assets >>workflow >>>href)
        try:
            cwl_href = stac_collection_raw["assets"]["cwl_script"]["href"]
            cwl_script = get_file_from_url(cwl_href)
            cwl_dict = yaml.safe_load(cwl_script)
        except KeyError:
            logging.warning(
                f"The STAC defintion provided in {file_name} is missing a cwl " "script href."
            )
            logging.info(
                "Continuing with workflow STAC creation, using UUID for Workflow ID and all "
                "default values"
            )
            # CWL reference not available, so cannot scrape the CWL script
            scrape_cwl = False
        except yaml.parser.ParserError as e:
            logging.error(
                f"The provided cwl script for {file_name} is not correctly formatted as yaml."
            )
            logging.error(repr(e))
            return
        except Exception as e:
            logging.warning(
                f"Failed to access cwl script defined in {file_name} at "
                f"{stac_collection_raw['assets']['cwl_script']['href']}, please ensure the url "
                "is available."
            )
            logging.warning(repr(e))
            logging.info(
                "Continuing with workflow STAC creation, using UUID for Workflow ID and all "
                "default values"
            )
            scrape_cwl = False

        # Get missing attributes from stac definition
        missing_fields = self.workflow_check_missing_fields(stac_collection_raw)

        if scrape_cwl:
            # Identify main workflow in cwl file
            try:
                for i in range(len(cwl_dict["$graph"])):
                    if cwl_dict["$graph"][i]["class"] == "Workflow":
                        cwl_workflow_position = i
                        break
                else:
                    logging.error(
                        f"The provided cwl script for {file_name} does not define a workflow in its graph."
                    )
                    return
            except KeyError as e:
                logging.error(
                    f"Unable to locate workflow within graph of cwl script for {file_name}"
                )
                logging.error(repr(e))
                return
            except Exception as e:
                logging.error(f"Failed to load workflow for {file_name}")
                logging.error(repr(e))
                return

        # For each missing field, we need to add it to the stac collection and attempt to complete it
        # automatically
        for field in missing_fields:
            if field == "extent":
                stac_collection_raw.update({"extent": DEFAULT_EXTENT_FIELD})
            elif field == "spatial":
                stac_collection_raw["extent"].update({"spatial": DEFAULT_EXTENT_FIELD["spatial"]})
            elif field == "bbox":
                stac_collection_raw["extent"]["spatial"].update(
                    {"bbox": DEFAULT_EXTENT_FIELD["spatial"]["bbox"]}
                )
            elif field == "temporal":
                stac_collection_raw["extent"].update({"temporal": DEFAULT_EXTENT_FIELD["temporal"]})
            elif field == "interval":
                stac_collection_raw["extent"]["temporal"].update(
                    {"interval": DEFAULT_EXTENT_FIELD["temporal"]["interval"]}
                )
            elif field == "inputs":
                # For some fields the script can scrape potential data from the cwl script itself
                if scrape_cwl and "inputs" in cwl_dict["$graph"][cwl_workflow_position]:
                    # Add an inputs field to the summaries object only when it can be populated
                    stac_collection_raw["summaries"].update({"inputs": None})
                    stac_collection_raw["summaries"]["inputs"] = cwl_dict["$graph"][
                        cwl_workflow_position
                    ]["inputs"]
            elif field == "outputs":
                # For some fields the script can scrape potential data from the cwl script itself
                if scrape_cwl and "outputs" in cwl_dict["$graph"][cwl_workflow_position]:
                    # Add an outputs field to the summaries object only when it can be populated
                    stac_collection_raw["summaries"].update({"outputs": None})
                    stac_collection_raw["summaries"]["outputs"] = cwl_dict["$graph"][
                        cwl_workflow_position
                    ]["outputs"]
            else:
                if field == "summaries":
                    stac_collection_raw.update({field: {}})
                elif field == "links":
                    stac_collection_raw.update({field: []})
                else:
                    stac_collection_raw.update({field: "N/A"})
                # For some fields the script can scrape potential data from the cwl script itself
                match field:
                    case "type":
                        stac_collection_raw["type"] = "Collection"
                    case "id":
                        # The id will be scraped from the cwl script or be generated randomly with a
                        # uuid if not provided as id in the cwl
                        id_uuid = (
                            stac_collection_raw["title"]
                            if "title" in stac_collection_raw and stac_collection_raw["title"]
                            else f"workflow__{uuid.uuid4()}"
                        )
                        stac_collection_raw["id"] = (
                            "workflow__" + cwl_dict["$graph"][cwl_workflow_position]["id"]
                            if scrape_cwl and "id" in cwl_dict["$graph"][cwl_workflow_position]
                            else id_uuid
                        )
                        logging.info(
                            "Generating STAC Collection for workflow "
                            f"{stac_collection_raw['id']}"
                        )
                    case "title":
                        # The title will be scraped from the cwl script or be generated randomly with a
                        # uuid if not provided as id in the cwl
                        title_uuid = (
                            stac_collection_raw["id"]
                            if "id" in stac_collection_raw and stac_collection_raw["id"]
                            else f"workflow__{uuid.uuid4()}"
                        )
                        stac_collection_raw["title"] = (
                            "workflow__" + cwl_dict["$graph"][cwl_workflow_position]["id"]
                            if scrape_cwl and "id" in cwl_dict["$graph"][cwl_workflow_position]
                            else title_uuid
                        )
                    case "stac_extensions":
                        stac_collection_raw["stac_extensions"] = []
                    case "stac_version":
                        stac_collection_raw["stac_version"] = DEFAULT_STAC_VERSION
                    case "description":
                        if scrape_cwl and "doc" in cwl_dict["$graph"][cwl_workflow_position]:
                            stac_collection_raw.update({"description": None})
                            stac_collection_raw["description"] = cwl_dict["$graph"][
                                cwl_workflow_position
                            ]["doc"]
                    case "license":
                        stac_collection_raw["license"] = "N/A"
                    case "keywords":
                        stac_collection_raw["keywords"] = ["workflow"]
                    case "summaries":
                        if scrape_cwl and "inputs" in cwl_dict["$graph"][cwl_workflow_position]:
                            stac_collection_raw["summaries"].update({"inputs": None})
                            stac_collection_raw["summaries"]["inputs"] = cwl_dict["$graph"][
                                cwl_workflow_position
                            ]["inputs"]
                        if scrape_cwl and "outputs" in cwl_dict["$graph"][cwl_workflow_position]:
                            stac_collection_raw["summaries"].update({"outputs": None})
                            stac_collection_raw["summaries"]["outputs"] = cwl_dict["$graph"][
                                cwl_workflow_position
                            ]["outputs"]
                    case _:
                        continue

        # Need to check links rel self is suitable for subsequent transformers, only if provided
        if "links" in stac_collection_raw:
            links = stac_collection_raw["links"]
            for link in links:
                if link["rel"] == "self":
                    link["href"] = source + file_name
                    break

        stac_collection = stac_collection_raw

        # Order STAC collection if required
        if ORDERED:
            stac_collection = collections.OrderedDict(
                (key, stac_collection_raw[key])
                for key in REQUIRED_COLLECTIONS_FIELDS
                if key in stac_collection
            )

        # Return json for further transform and upload
        return stac_collection
