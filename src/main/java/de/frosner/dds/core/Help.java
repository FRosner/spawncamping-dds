package de.frosner.dds.core;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface Help
{
    String   shortDescription();
    String   longDescription();
}
