package dev.buesing.ksd.restore;

import com.beust.jcommander.Parameter;
import dev.buesing.ksd.tools.config.BaseOptions;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Options extends BaseOptions {

    @Parameter(names = { "--changelog-topic" }, description = "")
    private String changelogTopic= "ABC-v1-product-purchased-store-changelog";

    @Parameter(names = { "--group-id" }, description = "")
    private String groupId= "restore";

}
