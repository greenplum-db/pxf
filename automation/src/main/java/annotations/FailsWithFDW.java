package annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Annotation for marking Test cases that fail when run against PXF FDW.
 *
 * This is a marker interface for now but can be used later to skip tests that cannot be run against FDW
 * (e.g. those exercising features of external tables that are not supported wth FDW).
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ METHOD, TYPE })
public @interface FailsWithFDW {
}
