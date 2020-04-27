namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsString
    {
        public static string GetCategoryStreamId(this string target)
            => $"{(target.StartsWith(Constants.CategoryPrefix) ? string.Empty : Constants.CategoryPrefix)}{target}";
    }
}