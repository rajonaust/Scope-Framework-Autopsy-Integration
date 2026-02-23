# -*- coding: utf-8 -*-

import json
import subprocess
import os
import time

from java.util import ArrayList
from java.lang import String

from org.sleuthkit.autopsy.ingest import IngestModuleFactoryAdapter
from org.sleuthkit.autopsy.ingest import DataSourceIngestModule
from org.sleuthkit.autopsy.ingest import IngestMessage
from org.sleuthkit.autopsy.ingest import IngestServices
from org.sleuthkit.autopsy.ingest import IngestModule
from org.sleuthkit.autopsy.casemodule import Case
from org.sleuthkit.datamodel import BlackboardArtifact, BlackboardAttribute
from org.sleuthkit.datamodel import Score
from org.sleuthkit.autopsy.ingest import ModuleDataEvent
from org.sleuthkit.datamodel import Blackboard

# Factory
class CommAccountMessageFactory(IngestModuleFactoryAdapter):

    def getModuleDisplayName(self):
        return "SCOPE Framework (Cosine_Dynamic_Threshold)"

    def getModuleDescription(self):
        return "Extracts only messages linked to Communication Accounts"

    def getModuleVersionNumber(self):
        return "1.0"

    def isDataSourceIngestModuleFactory(self):
        return True

    def createDataSourceIngestModule(self, ingestOptions):
        return CommAccountMessageModule()

# Module
class CommAccountMessageModule(DataSourceIngestModule):

    def process(self, dataSource, progressBar):

        case = Case.getCurrentCase()
        skCase = case.getSleuthkitCase()

        msgTypeID = skCase.getArtifactTypeID("TSK_MESSAGE")
        artifacts = skCase.getBlackboardArtifacts(msgTypeID)

        messages = []

        for art in artifacts:
            text = None
            app = None
            sender = None
            timestamp = None

            for attr in art.getAttributes():
                name = attr.getAttributeType().getTypeName()

                if name == "TSK_TEXT" and not text:
                    text = attr.getValueString()

                elif name == "TSK_PHONE_NUMBER_FROM" :
			sender = attr.getValueString()

		elif name == "TSK_MESSAGE_TYPE":
                    app = attr.getValueString()

                elif name == "TSK_DATETIME":
                    try:
                        timestamp = attr.getValueLong()
                    except:
                        timestamp = None

            # Only messages linked to Communication Accounts
            if text:
                messages.append({
                    "Chatroom": app if app else "Unknown",
                    "Sender": sender if sender else "Unknown",
                    "Timestamp": timestamp if timestamp else "Unknown",
		    "Text": text,
		    "Prompt": "None"
                })

        # Write JSON file
        exportDir = case.getExportDirectory()
        jsonPath = os.path.join(exportDir, "SCOPE.json")

        try:
            with open(jsonPath, "w") as f:
                json.dump(messages, f, indent=2)
        except Exception as e:
            IngestServices.getInstance().postMessage(
                IngestMessage.createMessage(
                    IngestMessage.MessageType.ERROR,
                    "Comm Messages to JSON",
                    "Failed to write JSON file"
                )
            )
            return IngestModule.ProcessResult.ERROR

	pythonExe = r"C:\Users\rbardhan\AppData\Local\Programs\Python\Python311\python.exe"      # FULL path to Python 3
	scopeScript = r"\\Mac\Shared Folder Windows\Forensics Tool Plug-In\Source Code\SCOPE with Cosine(Dynamic Threshold).py"         # FULL path to SCOPE.py
	workingDir = r"\\Mac\Shared Folder Windows\Forensics Tool Plug-In\Source Code" # Set this to your script's home

	# REGISTER CUSTOM ARTIFACT TYPE

        art_name = "TSK_SCOPE_SESSION_Cosine_Dynamic_Threshold"
        art_display = "SCOPE Framework (Cosine_Dynamic_Threshold)"
	art_type = skCase.getArtifactType(art_name)

	if art_type is not None:
            existing_arts = skCase.getBlackboardArtifacts(art_type.getTypeID(), dataSource.getId())
            if len(existing_arts) > 0:
                IngestServices.getInstance().postMessage(
                    IngestMessage.createMessage(
                        IngestMessage.MessageType.INFO, 
                        "SCOPE Framework (Cosine_Dynamic_Threshold)", 
                        "Analysis already completed for this source. Skipping to avoid duplicates."
                    )
                )
                return IngestModule.ProcessResult.OK

	try:
    		subprocess.call([
        		pythonExe,
        		scopeScript,
       			jsonPath
    		], cwd=workingDir)
	except:
    		IngestServices.getInstance().postMessage(
        		IngestMessage.createMessage(
            			IngestMessage.MessageType.WARNING,
            			"SCOPE Trigger",
            			"SCOPE could not be triggered"
        		)
    	)

	# Ouput to Autopsy

	scopeOutputPath = os.path.join(exportDir, "SCOPE_output.json")

	if not os.path.exists(scopeOutputPath):
    		return IngestModule.ProcessResult.OK
	
        case = Case.getCurrentCaseThrows()
        skCase = case.getSleuthkitCase()
	blackboard = skCase.getBlackboard()

	with open(scopeOutputPath, "r") as f:
        	scopeData = json.load(f)
	
	try:
		# Check if it already exists
		scopeArtifactType = blackboard.getOrAddArtifactType(art_name, art_display)
	except Exception as ex:
    		IngestServices.getInstance().postMessage(
        		IngestMessage.createMessage(IngestMessage.MessageType.WARNING, "SCOPE", "Artifact Type was None"+str(ex))
    		)

	def getAttr(name, attrType, display):
		return skCase.addArtifactAttributeType(name, attrType, display)

	def java_clean_text(text):
    		if text is None:
        		return String("")
    		try:
        		safe_bytes = text.encode("utf-8", "ignore")
        		return String(safe_bytes, "UTF-8")
    		except Exception:
        		return String("Text Error - Cleaned")

        ATTR_USER = getAttr("TSK_SCOPE_USER", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "User Name")
        ATTR_START = getAttr("TSK_SCOPE_START", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Start Time")
        ATTR_END = getAttr("TSK_SCOPE_END", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "End Time")
        ATTR_DURATION = getAttr("TSK_SCOPE_DURATION", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Conversation Duration")
        ATTR_TOPIC = getAttr("TSK_SCOPE_TOPIC", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Detected Topic")
        ATTR_PROB = getAttr("TSK_SCOPE_PROB", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.DOUBLE, "Topic Probability")
        ATTR_SUMMARY = getAttr("TSK_SCOPE_SUMMARY", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Chat Summary")

    	for entry in scopeData:

        	# Create the artifact
        	art = dataSource.newArtifact(scopeArtifactType.getTypeID())

        	# Create a Java list to hold attributes
        	attrs = ArrayList()

		try:
            		prob = float(entry.get("Probability", 0.0))
        	except:
            		prob = 0.0
    		
        	# Add attributes to the Java ArrayList individually
        	attrs.add(BlackboardAttribute(ATTR_USER, "SCOPE Framework (Cosine_Dynamic_Threshold)", str(entry.get("User", "Unknown"))))
		attrs.add(BlackboardAttribute(ATTR_START, "SCOPE Framework (Cosine_Dynamic_Threshold)", str(entry.get("Start Date", ""))))
            	attrs.add(BlackboardAttribute(ATTR_END, "SCOPE Framework (Cosine_Dynamic_Threshold)", str(entry.get("End Date", ""))))
            	attrs.add(BlackboardAttribute(ATTR_DURATION, "SCOPE Framework (Cosine_Dynamic_Threshold)", str(entry.get("Time Duration", "Unknown"))))
            	attrs.add(BlackboardAttribute(ATTR_TOPIC, "SCOPE Framework (Cosine_Dynamic_Threshold)", str(entry.get("Topic", "Unknown"))))
            	attrs.add(BlackboardAttribute(ATTR_PROB, "SCOPE Framework (Cosine_Dynamic_Threshold)", prob))
		attrs.add(BlackboardAttribute(ATTR_SUMMARY, "SCOPE Framework (Cosine_Dynamic_Threshold)",java_clean_text(entry.get("Chat Summary", ""))))

        	# Add the Java collection to the artifact
        	art.addAttributes(attrs)

        	# Add the artifact to the database
		try:
			blackboard.postArtifact(art, "SCOPE Framework (Cosine_Dynamic_Threshold)")
		except Exception as ex:
			IngestServices.getInstance().postMessage(
        			IngestMessage.createMessage(IngestMessage.MessageType.WARNING, "SCOPE", "Artifact Type was None"+str(ex))
    			)

	try: 
        # Fire the event
		if scopeArtifactType is not None:
            		IngestServices.getInstance().fireModuleDataEvent(
                		ModuleDataEvent("SCOPE Framework (Cosine_Dynamic_Threshold)", scopeArtifactType)
            		)
        	else:
            		IngestServices.getInstance().postMessage(
                		IngestMessage.createMessage(IngestMessage.MessageType.WARNING, "SCOPE", "Artifact Type was None")
           		)
    	except Exception as e:
       		IngestServices.getInstance().postMessage(
            		IngestMessage.createMessage(IngestMessage.MessageType.WARNING, "UI Refresh Error", str(e))
        	)

	IngestServices.getInstance().postMessage(
        			IngestMessage.createMessage(
            			IngestMessage.MessageType.INFO,
            			"SCOPE Import",
            			"SCOPE Analysis Completed Successfully."
        			)
    			)
    	
        return IngestModule.ProcessResult.OK
