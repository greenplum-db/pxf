package annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Annotation for marking Test cases that can be run against PXF FDW.
 *
 * This is temporary while we enable automation
 * to run against FDW as later most tests will be able to and there will be no need for this annotation.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ METHOD, TYPE })
public @interface WorksWithFDW {
}
