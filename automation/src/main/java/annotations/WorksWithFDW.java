package annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;

/**
 * Annotation for marking Test cases that can be run against PXF FDW.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ METHOD })
public @interface WorksWithFDW {
}
