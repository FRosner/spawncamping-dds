package de.frosner.dds.core;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface Help
{
    String category();
    String shortDescription();
    String longDescription();
    String parameters() default "";
    String parameters2() default "";
    String parameters3() default "";
    String parameters4() default "";
    String parameters5() default "";
    String parameters6() default "";
    String parameters7() default "";
    String parameters8() default "";
    String parameters9() default "";
}
