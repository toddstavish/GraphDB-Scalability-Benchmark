package bench;

import com.infinitegraph.BaseVertex;

class Person extends BaseVertex
{
    private String name;

    public Person(String name)
    {
        setName(name);
    }

    public void setName(String name)
    {
        markModified();
        this.name = name;
    }

    public String getName()
    {
        fetch();
        return this.name;
    }

}