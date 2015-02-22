package de.frosner.dds.core;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface HelpProvided
{
    String   shortDescription();
    String   longDescription();
}
