package de.frosner.dds.core;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class ManifestMetaData {

    private static Logger logger = Logger.getLogger("DDS");

    public static void logWelcomeMessage() {
        try {
            Class<ManifestMetaData> clazz = ManifestMetaData.class;
            String className = clazz.getSimpleName() + ".class";
            String classPath = clazz.getResource(className).toString();
            if (classPath.startsWith("jar")) {
                String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
                Manifest manifest;
                manifest = new Manifest(new URL(manifestPath).openStream());
                Attributes attr = manifest.getMainAttributes();

                logger.info("Initializing " + attr.getValue("Implementation-Title") + "-" +
                        attr.getValue("Implementation-Version") + " (" + attr.getValue("Git-Head-Rev") + ", " +
                        attr.getValue("Git-Branch") + ", " + attr.getValue("Build-Date") + ")");
            }
        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

}
