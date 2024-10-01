package tabletypesgen.util;

import java.util.List;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.Nullable;


public class Args
{
  private Args() {}

  public static int pluckIntOption(List<String> remArgs, String optionName, int defaultValue)
  {
    if ( remArgs.contains(optionName) )
    {
      int argIx = remArgs.indexOf(optionName);
      remArgs.remove(argIx);
      return Integer.parseInt(remArgs.remove(argIx));
    }

    return defaultValue;
  }

  public static Optional<String> pluckNonEmptyStringOption(List<String> remArgs, String optionName)
  {
    Optional<String> opt = pluckStringOption(remArgs, optionName);
    return opt.flatMap(s -> s.isEmpty() ? Optional.empty() : opt);
  }

  public static Optional<String> pluckStringOption(List<String> remArgs, String optionName)
  {
    int argIx = remArgs.indexOf(optionName);

    if ( argIx != -1 )
    {
      remArgs.remove(argIx);
      @Nullable String s = remArgs.remove(argIx);
      return s == null ? Optional.empty() : Optional.of(s);
    }

    return Optional.empty();
  }
}
