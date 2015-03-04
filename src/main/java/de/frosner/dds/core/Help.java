package de.frosner.dds.core;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to store the information required for printing help about a method.
 */
@Retention( RetentionPolicy.RUNTIME )
public @interface Help
{
    /**
     * @return Category of the method. It is used to group the methods in the overview.
     */
    String category();

    /**
     * @return Short description of the method. It it used to explain the functionality of the method in the overview.
     * It should not be longer than one sentence.
     */
    String shortDescription();

    /**
     * @return Long description of the method. It is used to explain the functionality of the method in detail when
     * help for individual methods is requested. It should contain a comprehensive user guide and possibly examples.
     */
    String longDescription();

    /**
     * @return Parameters the method takes, without paranthesis.
     * <br> Example: {@code parameters = "i: Int, s: String"}
     */
    String parameters() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters2() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters3() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters4() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters5() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters6() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters7() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters8() default "";

    /**
     * @return Curried parameters the method takes. They will be displayed behind the previous parameters with another
     * set of parenthesis.
     */
    String parameters9() default "";
}
